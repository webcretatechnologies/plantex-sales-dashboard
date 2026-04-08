from django.urls import path
from .views import FileUploadView

urlpatterns = [
    path('api/upload/', FileUploadView.as_view(), name='api-upload'),
]
