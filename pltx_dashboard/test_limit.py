import os
import django
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "pltx_dashboard.settings")
django.setup()

from apps.dashboard.models import DashboardProductDailySummary
from django.db.models import Sum

qs = DashboardProductDailySummary.objects.all()
top_asins = list(
    qs.exclude(sku__isnull=True)
    .exclude(sku="")
    .values("sku")
    .annotate(total_rev=Sum("revenue"))
    .order_by("-total_rev")[:10]
    .values_list("sku", flat=True)
)
print("Top 10 ASINs:", len(top_asins), top_asins)

rows = list(
    qs.filter(sku__in=top_asins)
    .values("sku", "date")
    .annotate(rev=Sum("revenue"))
)
print("Rows generated:", len(rows))
unique_skus = set(r["sku"] for r in rows)
print("Unique SKUs in rows:", len(unique_skus))
