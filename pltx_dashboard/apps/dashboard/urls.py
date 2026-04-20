from django.urls import path
from django.views.generic import RedirectView
from .views import (
    dashboard_view,
    upload_view,
    business_dashboard_view,
    ceo_dashboard_view,
    category_dashboard_view,
    download_calculated_data,
)

urlpatterns = [
    path(
        "",
        RedirectView.as_view(pattern_name="dashboard-home", permanent=False),
        name="root-redirect",
    ),
    path("dashboard/upload/", upload_view, name="dashboard-upload"),
    path("dashboard/", dashboard_view, name="dashboard-home"),
    path("dashboard/business/", business_dashboard_view, name="business-dashboard"),
    path("dashboard/ceo/", ceo_dashboard_view, name="ceo-dashboard"),
    path("dashboard/category/", category_dashboard_view, name="category-dashboard"),
    path(
        "dashboard/download/<str:file_format>/",
        download_calculated_data,
        name="dashboard-download",
    ),
]
