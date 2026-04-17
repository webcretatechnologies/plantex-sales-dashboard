from django.contrib import admin
from django.urls import path, include
from django.conf import settings
from django.conf.urls.static import static
from django.contrib.staticfiles.urls import staticfiles_urlpatterns
from django.http import JsonResponse

# Silences the automatic Chrome DevTools probe — returns {} instead of a 404.
def _chrome_devtools_noop(request):
    return JsonResponse({})

urlpatterns = [
    path('admin/', admin.site.urls),
    path('.well-known/appspecific/com.chrome.devtools.json', _chrome_devtools_noop),
    path('accounts/', include('apps.accounts.urls')),
    path('', include('apps.upload.urls')),
    path('', include('apps.dashboard.urls')),
    path('', include('apps.replenishment.urls')),
]

if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
    urlpatterns += staticfiles_urlpatterns()
