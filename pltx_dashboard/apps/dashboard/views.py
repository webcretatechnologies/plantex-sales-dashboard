from django.shortcuts import render, redirect
from apps.dashboard.models import SpendData, ProcessedDashboardData
from apps.dashboard.materialized_models import (
    CeoDashboardCache,
    BusinessDashboardCache,
    CategoryDashboardCache,
)
from apps.accounts.models import Users
import pandas as pd
import json
import numpy as np
from apps.dashboard.services.analytics_services import get_dashboard_payload
from apps.accounts.decorators import require_feature, _first_allowed_dashboard_for
from apps.accounts.models import Feature
class DashboardEncoder(json.JSONEncoder):
    """Handles numpy/pandas types that the default encoder chokes on."""
    def default(self, obj):
        if isinstance(obj, (np.integer,)):
            return int(obj)
        if isinstance(obj, (np.floating,)):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, (pd.Timestamp,)):
            return str(obj)
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        return super().default(obj)


def get_logged_in_user(request):
    """Get the logged-in Users instance from session, or None."""
    user_id = request.session.get('user_id')
    if not user_id:
        return None
    try:
        return Users.objects.get(id=user_id)
    except Users.DoesNotExist:
        return None


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

    # ── Try to get filter dropdown metadata from the materialized cache ──
    # This avoids loading the full unfiltered dataset just to populate dropdowns
    cached_filter_metadata = None
    cache = CeoDashboardCache.objects.filter(user=data_owner).first()
    if cache and cache.payload_json:
        cached_filter_metadata = cache.payload_json.get('filters', None)

    # ── Build the queryset with DB-level entity filters ──
    qs = ProcessedDashboardData.objects.filter(user=data_owner)

    # Apply category filter at DB level
    category = filters.get('category')
    if category:
        if isinstance(category, (list, tuple)):
            qs = qs.filter(category__in=category)
        else:
            qs = qs.filter(category=category)

    # Apply ASIN filter at DB level
    asin_filter = filters.get('asin')
    if asin_filter:
        if isinstance(asin_filter, (list, tuple)):
            qs = qs.filter(asin__in=asin_filter)
        else:
            qs = qs.filter(asin=asin_filter)

    # Apply portfolio filter at DB level
    portfolio = filters.get('portfolio')
    if portfolio:
        qs = qs.filter(portfolio=portfolio)

    # Apply subcategory filter at DB level
    subcategory = filters.get('subcategory')
    if subcategory:
        if isinstance(subcategory, (list, tuple)):
            qs = qs.filter(subcategory__in=subcategory)
        else:
            qs = qs.filter(subcategory=subcategory)

    # Note: platform filter is NOT applied at DB level because
    # ProcessedDashboardData may not have a 'platform' column.
    # It is handled safely in apply_global_filters() which checks column existence.

    if not qs.exists():
        return {
            'logged_user': user,
            'user_features': user_features,
            'payload': None,
            'filters': filters,
            'selected_filters': selected_filters,
            'selected_filters_json': json.dumps(selected_filters)
        }

    df = pd.DataFrame(list(qs.values()))

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


# ─────────────────────────────────────────────────────────
# Materialized-view helpers
# ─────────────────────────────────────────────────────────

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

