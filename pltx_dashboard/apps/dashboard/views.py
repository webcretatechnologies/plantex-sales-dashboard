from django.shortcuts import render, redirect
from django.http import FileResponse
from apps.dashboard.models import SpendData, ProcessedDashboardData, FlipkartProcessedDashboardData
from apps.dashboard.materialized_models import (
    CeoDashboardCache,
    BusinessDashboardCache,
    CategoryDashboardCache,
)
from apps.accounts.models import Users
import pandas as pd
import json
from apps.dashboard.services.analytics_services import get_dashboard_payload
from apps.accounts.decorators import require_feature, _first_allowed_dashboard_for
from apps.accounts.models import Feature
from apps.dashboard.utils import DashboardEncoder


from apps.accounts.utils import get_logged_in_user


def dashboard_view(request):
    # Redirect the user to the first dashboard they have access to.
    user = get_logged_in_user(request)
    if not user:
        return redirect('account-login')
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
        user_features = [f.code_name for f in user.role.features.all()] if user.role else []

    # Build filters from QueryDict preserving repeated params as lists
    filters = {}
    for k in request.GET.keys():
        vals = request.GET.getlist(k)
        if len(vals) == 1:
            filters[k] = vals[0]
        else:
            filters[k] = vals

    # selected_filters is used by templates to pre-select multi-select controls (always lists)
    selected_filters = {
        'categories': request.GET.getlist('category'),
        'asins': request.GET.getlist('asin'),
    }

    # Filter dropdown metadata from cache
    cached_filter_metadata = None
    cache = CeoDashboardCache.objects.filter(user=data_owner).first()
    if cache and cache.payload_json:
        cached_filter_metadata = cache.payload_json.get('filters', None)

    # Build the queryset with DB-level entity filters
    qs = ProcessedDashboardData.objects.filter(user=data_owner)
    fk_qs = FlipkartProcessedDashboardData.objects.filter(user=data_owner)

    # Apply platform filter
    platform = filters.get('platform')
    show_amazon = True
    show_flipkart = True
    if platform == 'Amazon':
        show_flipkart = False
    elif platform == 'Flipkart':
        show_amazon = False

    # Apply category filter at DB level
    category = filters.get('category')
    if category:
        if isinstance(category, (list, tuple)):
            qs = qs.filter(category__in=category)
            fk_qs = fk_qs.filter(category__in=category)
        else:
            qs = qs.filter(category=category)
            fk_qs = fk_qs.filter(category=category)

    # Apply ASIN filter at DB level
    asin_filter = filters.get('asin')
    if asin_filter:
        if isinstance(asin_filter, (list, tuple)):
            qs = qs.filter(asin__in=asin_filter)
            fk_qs = fk_qs.filter(fsn__in=asin_filter)
        else:
            qs = qs.filter(asin=asin_filter)
            fk_qs = fk_qs.filter(fsn=asin_filter)

    # Apply portfolio filter at DB level
    portfolio = filters.get('portfolio')
    if portfolio:
        qs = qs.filter(portfolio=portfolio)
        fk_qs = fk_qs.filter(portfolio=portfolio)

    # Apply subcategory filter at DB level
    subcategory = filters.get('subcategory')
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
            'logged_user': user,
            'user_features': user_features,
            'payload': None,
            'filters': filters,
            'selected_filters': selected_filters,
            'selected_filters_json': json.dumps(selected_filters)
        }

    df = pd.DataFrame()
    if qs.exists():
        df = pd.DataFrame(list(qs.values()))
        # Add platform column for Amazon data
        df['platform'] = 'Amazon'

    if fk_qs.exists():
        df_fk = pd.DataFrame(list(fk_qs.values()))
        # Rename fsn → asin so the analytics layer works uniformly
        df_fk = df_fk.rename(columns={'fsn': 'asin'})
        df_fk['platform'] = 'Flipkart'
        # Ensure matching columns
        for col in ['spend_sp', 'spend_sb', 'spend_sd']:
            if col not in df_fk.columns:
                df_fk[col] = 0.0
        
        if df.empty:
            df = df_fk
        else:
            # Concat
            common_cols = [c for c in df.columns if c in df_fk.columns]
            df = pd.concat([df[common_cols], df_fk[common_cols]], ignore_index=True)

    # Apply same entity filters to spend data at DB level
    spend_qs = SpendData.objects.filter(user=data_owner)
    if asin_filter:
        if isinstance(asin_filter, (list, tuple)):
            spend_qs = spend_qs.filter(asin__in=asin_filter)
        else:
            spend_qs = spend_qs.filter(asin=asin_filter)
    spend_df = pd.DataFrame(list(spend_qs.values())) if spend_qs.exists() else pd.DataFrame()

    payload = get_dashboard_payload(
        df, spend_df, filters, data_owner,
        cached_filter_metadata=cached_filter_metadata,
    )
    return {
        'logged_user': user,
        'user_features': user_features,
        'payload': payload,
        'payload_json': json.dumps(payload, cls=DashboardEncoder),
        'filters': filters,
        'selected_filters': selected_filters,
        'selected_filters_json': json.dumps(selected_filters)
    }


# Materialized-view helpers

from apps.dashboard.materialized_models import (
    DashboardFilterCache,
    STANDARD_DATE_RANGES,
)


def _get_cacheable_filter_key(request):
    """
    Return the cache key if this request uses ONLY a standard date-range
    filter (no category, ASIN, platform, or custom dates).
    Returns None if the request can't be served from the filter cache.
    """
    params = request.GET
    if not params:
        return None  # No filters → handled by the all-time cache

    date_range = params.get('date_range', '')
    if not date_range or date_range not in STANDARD_DATE_RANGES:
        return None  # Custom dates or unknown range

    # Check that no OTHER meaningful filters are applied
    # (start_date/end_date/platform may appear as empty strings from the form)
    for key, val in params.items():
        if key == 'date_range':
            continue
        # Skip empty string values (form artefacts)
        if isinstance(val, str) and not val.strip():
            continue
        # Any other non-empty param means we can't use the cache
        return None

    return date_range


def _build_cached_context(user, payload, refreshed_at, filters=None, selected_filters=None):
    """
    Build a template context from a pre-computed payload.
    Avoids all Pandas / analytics computation.
    """
    if user.is_main_user:
        user_features = [f.code_name for f in Feature.objects.all()]
    else:
        user_features = [f.code_name for f in user.role.features.all()] if user.role else []

    if filters is None:
        filters = {}
    if selected_filters is None:
        selected_filters = {'categories': [], 'asins': []}

    return {
        'logged_user': user,
        'user_features': user_features,
        'payload': payload,
        'payload_json': json.dumps(payload, cls=DashboardEncoder),
        'filters': filters,
        'selected_filters': selected_filters,
        'selected_filters_json': json.dumps(selected_filters),
        'cache_refreshed_at': refreshed_at,
    }


def _try_serve_from_cache(request, user, cache_model):
    """
    Try to serve the dashboard from cache:
      1. No filters → all-time cache
      2. Standard date-range only → filter cache
      3. Otherwise → return None (live computation needed)
    """
    data_owner = user.created_by if user.created_by else user

    if not request.GET:
        # No filters → all-time cache
        cache = cache_model.objects.filter(user=data_owner).first()
        if cache and cache.payload_json:
            return _build_cached_context(user, cache.payload_json, cache.refreshed_at)
        return None

    # Check for a standard date-range-only request
    filter_key = _get_cacheable_filter_key(request)
    if filter_key:
        fc = DashboardFilterCache.objects.filter(
            user=data_owner, filter_key=filter_key
        ).first()
        if fc and fc.payload_json:
            return _build_cached_context(
                user, fc.payload_json, fc.refreshed_at,
                filters={'date_range': filter_key},
            )

    return None


# ─────────────────────────────────────────────────────────
# Dashboard views — cache-first, live-fallback
# ─────────────────────────────────────────────────────────

@require_feature('business_dashboard')
def business_dashboard_view(request):
    user = get_logged_in_user(request)
    if not user:
        return redirect('account-login')

    ctx = _try_serve_from_cache(request, user, BusinessDashboardCache)
    if ctx:
        return render(request, 'dashboard/business_dashboard.html', ctx)

    # Fallback to live computation
    ctx = get_dashboard_context(request)
    if ctx is None: return redirect('account-login')
    return render(request, 'dashboard/business_dashboard.html', ctx)

@require_feature('ceo_dashboard')
def ceo_dashboard_view(request):
    user = get_logged_in_user(request)
    if not user:
        return redirect('account-login')

    ctx = _try_serve_from_cache(request, user, CeoDashboardCache)
    if ctx:
        return render(request, 'dashboard/ceo_dashboard.html', ctx)

    # Fallback to live computation
    ctx = get_dashboard_context(request)
    if ctx is None: return redirect('account-login')
    return render(request, 'dashboard/ceo_dashboard.html', ctx)

@require_feature('category_dashboard')
def category_dashboard_view(request):
    user = get_logged_in_user(request)
    if not user:
        return redirect('account-login')

    ctx = _try_serve_from_cache(request, user, CategoryDashboardCache)
    if ctx:
        return render(request, 'dashboard/category_dashboard.html', ctx)

    # Fallback to live computation
    ctx = get_dashboard_context(request)
    if ctx is None: return redirect('account-login')
    return render(request, 'dashboard/category_dashboard.html', ctx)

@require_feature('upload_data')
def upload_view(request):
    user = get_logged_in_user(request)
    if not user:
        return redirect('account-login')
    
    if user.is_main_user:
        user_features = [f.code_name for f in Feature.objects.all()]
    else:
        user_features = [f.code_name for f in user.role.features.all()] if user.role else []
        
    return render(request, 'dashboard/upload.html', {'logged_user': user, 'user_features': user_features})


def download_calculated_data(request, file_format):
    """Download the calculated/merged dashboard data as CSV or Excel.
    
    Uses the same filters currently applied on the dashboard.
    The export mirrors the logic from scripts/cleaning_mapping_merging.py.
    """
    from apps.dashboard.services.export_services import export_csv, export_excel
    from datetime import datetime

    user = get_logged_in_user(request)
    if not user:
        return redirect('account-login')

    # Collect filters from query params (same as dashboard views)
    filters = {}
    for k in request.GET.keys():
        vals = request.GET.getlist(k)
        if len(vals) == 1:
            filters[k] = vals[0]
        else:
            filters[k] = vals

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    if file_format == 'csv':
        buf = export_csv(user, filters)
        response = FileResponse(buf, content_type='text/csv')
        response['Content-Disposition'] = f'attachment; filename="Calculated_Dashboard_Data_{timestamp}.csv"'
        return response
    elif file_format == 'excel':
        buf = export_excel(user, filters)
        response = FileResponse(
            buf,
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        response['Content-Disposition'] = f'attachment; filename="Calculated_Dashboard_Data_{timestamp}.xlsx"'
        return response
    else:
        from django.http import JsonResponse
        return JsonResponse({"error": "Invalid format. Use 'csv' or 'excel'."}, status=400)

