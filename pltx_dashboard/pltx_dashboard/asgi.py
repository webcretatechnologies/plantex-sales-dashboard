import os
from django.core.asgi import get_asgi_application
from channels.routing import ProtocolTypeRouter, URLRouter
from channels.auth import AuthMiddlewareStack
from channels.security.websocket import OriginValidator
import apps.upload.routing

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "pltx_dashboard.settings")

django_asgi_app = get_asgi_application()

# Allowed WebSocket origins — must match the protocol + host the browser connects from.
# This should stay in sync with settings.CSRF_TRUSTED_ORIGINS.
WEBSOCKET_ALLOWED_ORIGINS = [
    "https://admin.plantex.work",
    "http://admin.plantex.work",
    "https://209.182.233.109",
    "http://209.182.233.109",
    "https://desk.sapiosol.com",
    "http://desk.sapiosol.com",
    # Allow connections from the same machine / Docker internal network
    "http://localhost",
    "http://127.0.0.1",
]

application = ProtocolTypeRouter(
    {
        "http": django_asgi_app,
        "websocket": OriginValidator(
            AuthMiddlewareStack(URLRouter(apps.upload.routing.websocket_urlpatterns)),
            WEBSOCKET_ALLOWED_ORIGINS,
        ),
    }
)
