from django.shortcuts import render, redirect
from apps.dashboard.models import SpendData, ProcessedDashboardData
from apps.accounts.models import Users
import pandas as pd
import json
import numpy as np
from apps.dashboard.services.analytics_services import get_dashboard_payload
from apps.accounts.decorators import require_feature
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
    return redirect('business-dashboard')

def get_dashboard_context(request):
    user = get_logged_in_user(request)
    if not user:
        return None

    data_owner = user.created_by if user.created_by else user

    if user.is_main_user:
        user_features = [f.code_name for f in Feature.objects.all()]
    else:
        user_features = [f.code_name for f in user.role.features.all()] if user.role else []

    filters = request.GET.dict()
    qs = ProcessedDashboardData.objects.filter(user=data_owner).values()
    if not qs:
        return {'logged_user': user, 'user_features': user_features, 'payload': None}
        
    df = pd.DataFrame(list(qs))
    spend_qs = SpendData.objects.filter(user=data_owner).values()
    spend_df = pd.DataFrame(list(spend_qs)) if spend_qs else pd.DataFrame()
    
    payload = get_dashboard_payload(df, spend_df, filters, data_owner)
    return {'logged_user': user, 'user_features': user_features, 'payload': payload, 'payload_json': json.dumps(payload, cls=DashboardEncoder)}


@require_feature('business_dashboard')
def business_dashboard_view(request):
    ctx = get_dashboard_context(request)
    if ctx is None: return redirect('account-login')
    return render(request, 'dashboard/business_dashboard.html', ctx)

@require_feature('ceo_dashboard')
def ceo_dashboard_view(request):
    ctx = get_dashboard_context(request)
    if ctx is None: return redirect('account-login')
    return render(request, 'dashboard/ceo_dashboard.html', ctx)

@require_feature('category_dashboard')
def category_dashboard_view(request):
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



