from django.urls import path
from .views import FileUploadView, UploadTaskStatusView

urlpatterns = [
    path("api/upload/", FileUploadView.as_view(), name="api-upload"),
    path(
        "api/upload/status/<str:task_id>/",
        UploadTaskStatusView.as_view(),
        name="api-upload-status",
    ),
]
