import os
from django.core.asgi import get_asgi_application
from channels.routing import ProtocolTypeRouter, URLRouter
from channels.auth import AuthMiddlewareStack
import apps.upload.routing

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "pltx_dashboard.settings")

django_asgi_app = get_asgi_application()

# NOTE: No OriginValidator here: this app is deployed behind nginx, and the
# upload progress consumer enforces session-based websocket access.
application = ProtocolTypeRouter(
    {
        "http": django_asgi_app,
        "websocket": AuthMiddlewareStack(
            URLRouter(apps.upload.routing.websocket_urlpatterns)
        ),
    }
)
