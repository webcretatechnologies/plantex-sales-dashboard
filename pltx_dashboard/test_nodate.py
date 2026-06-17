import os
import django
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "pltx_dashboard.settings")
django.setup()

from apps.dashboard.models import DashboardProductDailySummary
from django.db.models import Sum

qs = DashboardProductDailySummary.objects.filter(platform="Amazon")
top_skus = list(
    qs.exclude(sku__isnull=True)
    .exclude(sku="")
    .values("sku")
    .annotate(total_rev=Sum("revenue"))
    .order_by("-total_rev")[:10]
    .values_list("sku", flat=True)
)

rows = list(
    qs.filter(sku__in=top_skus)
    .values("sku")
    .annotate(rev=Sum("revenue"), units=Sum("units_sold"))
    .order_by("-rev")
)
print("Rows generated:", len(rows))
for r in rows:
    print(r)
