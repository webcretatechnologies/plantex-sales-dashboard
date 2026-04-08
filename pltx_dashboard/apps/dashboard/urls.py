from django.urls import path
from .views import (
    dashboard_view, upload_view,
    business_dashboard_view, ceo_dashboard_view, category_dashboard_view
)

urlpatterns = [
    path('', upload_view, name='dashboard-upload'),
    path('dashboard/', dashboard_view, name='dashboard-home'),
    path('dashboard/business/', business_dashboard_view, name='business-dashboard'),
    path('dashboard/ceo/', ceo_dashboard_view, name='ceo-dashboard'),
    path('dashboard/category/', category_dashboard_view, name='category-dashboard'),
]
