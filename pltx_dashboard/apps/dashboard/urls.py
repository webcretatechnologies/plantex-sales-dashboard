from django.urls import path
from django.views.generic import RedirectView
from .views import (
    dashboard_view,
    upload_view,
    business_dashboard_view,
    ceo_dashboard_view,
    category_dashboard_view,
    download_calculated_data,
    download_demo_template,
    dashboard_section_view,
    dashboard_modal_rows_view,
    dashboard_category_performance_rows_view,
    filter_dropdown_options,
    dashboard_refresh_status,
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
        "dashboard/sections/<str:view_name>/<str:section>/",
        dashboard_section_view,
        name="dashboard-section",
    ),
    path(
        "dashboard/modal-rows/<str:view_name>/<str:modal_key>/",
        dashboard_modal_rows_view,
        name="dashboard-modal-rows",
    ),
    path(
        "dashboard/category-performance-rows/<str:view_name>/",
        dashboard_category_performance_rows_view,
        name="dashboard-category-performance-rows",
    ),
    path(
        "api/dashboard/filter-options/",
        filter_dropdown_options,
        name="dashboard-filter-options",
    ),
    path(
        "api/dashboard/refresh-status/",
        dashboard_refresh_status,
        name="dashboard-refresh-status",
    ),
    path(
        "dashboard/download/<str:file_format>/",
        download_calculated_data,
        name="dashboard-download",
    ),
    path("api/demo-template/", download_demo_template, name="dashboard-demo-template"),
]
