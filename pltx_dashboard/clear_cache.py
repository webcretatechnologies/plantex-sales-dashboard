import os

import django
from django.core.cache import cache


os.environ.setdefault("DJANGO_SETTINGS_MODULE", "pltx_dashboard.settings")
django.setup()

cache.clear()
print("Cache cleared.")
