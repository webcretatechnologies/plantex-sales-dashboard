from apps.dashboard.services.exports import (_clean_export_value, _rows_to_export_table, _npd_export_table, _modal_rows_export_filename, _category_performance_export_filename, _asin_fsn_report_export_filename, _asin_fsn_report_export_table, _category_performance_export_table)
import csv
import datetime
import json
import hashlib
import math
import time
from copy import deepcopy
from io import BytesIO, StringIO

import pandas as pd
from django.conf import settings
from django.core.cache import cache
from django.db.models import F, Max, Q, Sum, Case, When, Value, IntegerField
from django.http import FileResponse, HttpResponse, JsonResponse
from django.shortcuts import render, redirect
from django.template.loader import render_to_string
from django.utils import timezone
from django.views.decorators.http import require_GET

from apps.accounts.decorators import require_feature, _first_allowed_dashboard_for
from apps.accounts.models import Feature
from apps.accounts.utils import get_logged_in_user
from apps.dashboard.models import (
    SalesData,
    SpendData,
    ProcessedDashboardData,
    FlipkartProcessedDashboardData,
    FlipkartSearchTraffic,
    FlipkartPLA,
    DashboardInventoryHealthSummary,
    DashboardProductDailySummary,
    DashboardDailySummary,
    CategoryMapping,
    FlipkartCategoryMap,
)
from apps.dashboard.services.filters import (
    LIST_FILTER_FIELDS,
    apply_dashboard_entity_filters,
    build_filters_from_querydict,
    cache_filter_string,
    normalize_payload_filters,
    resolve_product_entity_allow_lists,
    selected_filter_payload,
)
from apps.dashboard.services.materialized_cache import (
    get_materialized_summary,
    store_materialized_summary,
)
from apps.dashboard.services.invalidation import invalidate_dashboard_cache_for_user
from apps.dashboard.services.cache_config import DASHBOARD_PAYLOAD_CACHE_VERSION
from apps.dashboard.services.cache_config import (
    DASHBOARD_CACHE_TTL_FULL_SECONDS,
    DASHBOARD_CACHE_TTL_LITE_SECONDS,
    DASHBOARD_CACHE_SCHEMA_VERSION,
)
from apps.dashboard.utils import DashboardEncoder

DASHBOARD_FEATURE_BY_VIEW = {
    "business": "business_dashboard",
    "ceo": "ceo_dashboard",
    "category": "category_dashboard",
}

DASHBOARD_SECTION_TEMPLATE_MAP = {
    ("business", "overview"): "dashboard/sections/business/overview.html",
    ("business", "visuals"): "dashboard/sections/business/visuals.html",
    ("business", "details"): "dashboard/sections/business/details.html",
    ("ceo", "overview"): "dashboard/sections/ceo/overview.html",
    ("ceo", "visuals"): "dashboard/sections/ceo/visuals.html",
    ("ceo", "details"): "dashboard/sections/ceo/details.html",
    ("category", "overview"): "dashboard/sections/category/overview.html",
    ("category", "visuals"): "dashboard/sections/category/visuals.html",
    ("category", "details"): "dashboard/sections/category/details.html",
}

DASHBOARD_MODAL_ROWS_TEMPLATE_MAP = {
    ("business", "category-growth"): ("dashboard/modals/rows/category_growth_rows.html", "category_performance"),
    ("ceo", "inventory-health"): ("dashboard/modals/rows/inventory_health_rows.html", "inventory.details"),
    ("ceo", "top-products"): ("dashboard/modals/rows/top_products_simple_rows.html", "cat_all_top_products"),
    ("ceo", "npd-performance"): ("dashboard/modals/rows/npd_performance_rows.html", "npd_products_all"),
    ("ceo", "declining-products"): ("dashboard/modals/rows/declining_products_rows.html", "cat_all_under_products"),
    ("business", "inventory-health"): ("dashboard/modals/rows/inventory_health_rows.html", "inventory.details"),
    ("business", "top-products"): ("dashboard/modals/rows/top_products_simple_rows.html", "cat_all_top_products"),
    ("business", "npd-performance"): ("dashboard/modals/rows/npd_performance_rows.html", "npd_products_all"),
    ("business", "declining-products"): ("dashboard/modals/rows/declining_products_rows.html", "cat_all_under_products"),
    ("category", "cluster-performance"): ("dashboard/modals/rows/cluster_performance_rows.html", "cluster_performance"),
    ("category", "inventory-health"): ("dashboard/modals/rows/inventory_health_rows.html", "inventory.details"),
    ("category", "top-products"): ("dashboard/modals/rows/top_products_simple_rows.html", "cat_all_top_products"),
    ("category", "npd-performance"): ("dashboard/modals/rows/npd_performance_rows.html", "npd_products_all"),
    ("category", "declining-products"): ("dashboard/modals/rows/declining_products_rows.html", "cat_all_under_products"),
}
DASHBOARD_PRODUCT_CARD_TEMPLATE_MAP = {
    ("business", "top-products"): "dashboard/partials/product_cards/top_products_simple_rows.html",
    ("business", "declining-products"): "dashboard/partials/product_cards/declining_products_rows.html",
    ("business", "npd-performance"): "dashboard/partials/product_cards/npd_performance_rows.html",
    ("ceo", "top-products"): "dashboard/partials/product_cards/top_products_simple_rows.html",
    ("ceo", "declining-products"): "dashboard/partials/product_cards/declining_products_rows.html",
    ("ceo", "npd-performance"): "dashboard/partials/product_cards/npd_performance_rows.html",
    ("category", "top-products"): "dashboard/partials/product_cards/top_products_simple_rows.html",
    ("category", "declining-products"): "dashboard/partials/product_cards/declining_products_rows.html",
    ("category", "npd-performance"): "dashboard/partials/product_cards/npd_performance_rows.html",
}

DASHBOARD_PRODUCT_CARD_PAYLOAD_KEY_MAP = {
    "top-products": "cat_top_products",
    "declining-products": "cat_under_products",
    "npd-performance": "npd_products",
}

DASHBOARD_CATEGORY_PERFORMANCE_ROWS_TEMPLATE_MAP = {
    "business": "dashboard/partials/category_performance_rows_business.html",
    "ceo": "dashboard/partials/category_performance_rows_ceo.html",
    "category": "dashboard/partials/category_performance_rows_category.html",
}


def _build_payload_json(payload):
    """
    Return full payload JSON for frontend consumers.
    """
    if not payload:
        return "null"
    return json.dumps(payload, cls=DashboardEncoder, separators=(",", ":"))


def _resolve_payload_key(payload, payload_key):
    rows = payload
    for part in str(payload_key).split("."):
        if isinstance(rows, dict):
            rows = rows.get(part)
        else:
            return []
    return rows or []








def _resolve_asin_fsn_report_bounds(date_range, start_date=None, end_date=None):
    date_range = str(date_range or "all").strip().lower()
    today = timezone.localdate()

    if date_range == "custom":
        start = None
        end = None
        try:
            if start_date:
                start = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
            if end_date:
                end = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
        except ValueError:
            pass
        return start, end

    if date_range == "last_7_days":
        return today - datetime.timedelta(days=6), today
    if date_range == "last_14_days":
        return today - datetime.timedelta(days=13), today
    if date_range == "last_21_days":
        return today - datetime.timedelta(days=20), today
    if date_range == "last_28_days":
        return today - datetime.timedelta(days=27), today
    if date_range == "last_month":
        current_month_start = today.replace(day=1)
        end = current_month_start - datetime.timedelta(days=1)
        return end.replace(day=1), end
    if date_range == "current_month":
        return today.replace(day=1), today
    return None, None


def _apply_asin_fsn_report_date_range(qs, date_range, start_date=None, end_date=None):
    start, end = _resolve_asin_fsn_report_bounds(date_range, start_date, end_date)
    
    if date_range == "custom":
        if start and not end:
            end = start
        elif end and not start:
            start = end

    if start and end:
        return qs.filter(date__gte=start, date__lte=end)
    elif start:
        return qs.filter(date__gte=start)
    elif end:
        return qs.filter(date__lte=end)
    return qs


def _asin_fsn_monthly_filters(filters, report_date_range):
    report_date_range = str(report_date_range or "all").strip().lower()
    if report_date_range not in {"all", "last_month", "current_month"}:
        return None

    monthly_filters = dict(filters or {})
    monthly_filters.pop("date_range", None)
    monthly_filters.pop("start_date", None)
    monthly_filters.pop("end_date", None)
    if report_date_range == "last_month":
        monthly_filters["date_range"] = "last_month"
    elif report_date_range == "current_month":
        today = timezone.localdate()
        monthly_filters["date_range"] = "custom"
        monthly_filters["start_date"] = today.replace(day=1).isoformat()
        monthly_filters["end_date"] = today.isoformat()
    return monthly_filters






def _strip_non_dashboard_filters(filters):
    cleaned = dict(filters or {})
    keys_to_remove = {
        "scope",
        "q",
        "page",
        "page_size",
        "export",
        "report_limit",
        "report_date_range",
        "draw",
        "start",
        "length",
        "order_field",
        "order_dir",
        "_",
        "all",
    }
    for key in list(cleaned.keys()):
        if key in keys_to_remove or key.startswith("search[") or key.startswith("order[") or key.startswith("columns["):
            cleaned.pop(key, None)
    return cleaned


def _list_or_scalar_filter(qs, field_name, value):
    if not value:
        return qs
    if isinstance(value, (list, tuple, set)):
        values = [str(item) for item in value if str(item).strip()]
        return qs.filter(**{f"{field_name}__in": values}) if values else qs
    return qs.filter(**{field_name: value})


def _modal_row_text(row):
    if isinstance(row, dict):
        return " ".join(_clean_export_value(value).lower() for value in row.values())
    return _clean_export_value(row).lower()


def _filter_rows_by_query(rows, query):
    if not query:
        return rows
    needle = str(query).strip().lower()
    if not needle:
        return rows
    return [row for row in rows if needle in _modal_row_text(row)]


def _paginate_rows(rows, page, page_size):
    total = len(rows)
    start = (page - 1) * page_size
    return total, rows[start : start + page_size]


def _sort_rows_by_field(rows, field, direction):
    """Sort a list of row dicts by a field name. direction: 'asc' or 'desc'."""
    if not field or not rows:
        return rows
    reverse = (str(direction).lower() == "desc")
    def _key(row):
        v = row.get(field)
        if v is None:
            return (1, 0, "")  # None sorts last
        if isinstance(v, (int, float)):
            return (0, v, "")
        try:
            return (0, float(v), "")
        except (TypeError, ValueError):
            return (0, 0, str(v).lower())
    try:
        return sorted(rows, key=_key, reverse=reverse)
    except Exception:
        return rows


def _extract_dt_cells_from_html(html):
    """
    Parse HTML containing <tr>…</tr> rows and return a list of row-arrays,
    where each row-array is a list of full <td>…</td> HTML strings.
    Used for DataTables serverSide responses that pre-render cell HTML.
    """
    from html.parser import HTMLParser

    class _Extractor(HTMLParser):
        def __init__(self):
            super().__init__()
            self.rows = []
            self._row = None
            self._cell = None
            self._in_tr = False
            self._in_td = False

        def handle_starttag(self, tag, attrs):
            tag = tag.lower()
            if tag == "tr" and not self._in_tr:
                self._in_tr = True
                self._row = []
                return
            if not self._in_tr:
                return
            if tag == "td" and not self._in_td:
                self._in_td = True
                self._cell = [self._rebuild(tag, attrs)]
                return
            if self._in_td:
                self._cell.append(self._rebuild(tag, attrs))

        def handle_endtag(self, tag):
            tag = tag.lower()
            if self._in_td:
                if tag == "td":
                    self._cell.append("</td>")
                    if self._row is not None:
                        self._row.append("".join(self._cell))
                    self._in_td = False
                    self._cell = None
                    return
                self._cell.append(f"</{tag}>")
                return
            if tag == "tr" and self._in_tr:
                if self._row:
                    self.rows.append(self._row)
                self._in_tr = False
                self._row = None

        def handle_data(self, data):
            if self._in_td and self._cell is not None:
                self._cell.append(data)

        def handle_entityref(self, name):
            if self._in_td and self._cell is not None:
                self._cell.append(f"&{name};")

        def handle_charref(self, name):
            if self._in_td and self._cell is not None:
                self._cell.append(f"&#{name};")

        @staticmethod
        def _rebuild(tag, attrs):
            parts = [f"<{tag}"]
            for name, val in attrs:
                if val is None:
                    parts.append(f" {name}")
                else:
                    parts.append(f" {name}=\"{val}\"")
            parts.append(">")
            return "".join(parts)

    ex = _Extractor()
    ex.feed(str(html or ""))
    return ex.rows


def _get_filtered_processed_querysets(data_owner, filters):
    qs = ProcessedDashboardData.objects.filter(user=data_owner)
    fk_qs = FlipkartProcessedDashboardData.objects.filter(user=data_owner)
    return apply_dashboard_entity_filters(qs, fk_qs, filters, user=data_owner)



def _get_asin_fsn_report_product_daily_rows(data_owner, filters, report_date_range, report_limit, report_start_date=None, report_end_date=None):
    # Summary table has product ids and dashboard dimensions, but not mapping-only
    # filters such as CM/material/rating/launch-date. Preserve exact behavior by
    # falling back to the raw processed tables for those cases.
    unsupported_fields = (
        "parent_asin",
        "sku",
        "category_manager",
        "series_name",
        "material",
        "size",
        "brand_name",
        "ratings",
        "finish",
        "inventory_health",
        "launch_date_range",
        "launch_start_date",
        "launch_end_date",
    )
    if any(filters.get(field) for field in unsupported_fields):
        return None

    qs = DashboardProductDailySummary.objects.filter(user=data_owner)
    qs = _apply_asin_fsn_report_date_range(qs, report_date_range, report_start_date, report_end_date)
    qs = _list_or_scalar_filter(qs, "category", filters.get("category"))
    qs = _list_or_scalar_filter(qs, "portfolio", filters.get("portfolio"))
    qs = _list_or_scalar_filter(qs, "subcategory", filters.get("subcategory"))

    platform = filters.get("platform") or "All"
    show_amazon = platform != "Flipkart"
    show_flipkart = platform != "Amazon"
    entity_asins, entity_fsns = resolve_product_entity_allow_lists(data_owner, filters)

    asin_rows = []
    fsn_rows = []
    combined_totals = []

    from concurrent.futures import ThreadPoolExecutor

    def fetch_az_totals():
        if show_amazon:
            az_qs = qs.filter(platform="Amazon")
            if entity_asins is not None:
                if not entity_asins:
                    return []
                az_qs = _list_or_scalar_filter(az_qs, "asin", entity_asins)
            return list(
                az_qs.exclude(sku__isnull=True)
                .exclude(sku="")
                .values("sku")
                .annotate(total_rev=Sum("revenue"))
                .order_by("-total_rev")[:report_limit]
            )
        return []

    def fetch_fk_totals():
        if show_flipkart:
            fk_qs = qs.filter(platform="Flipkart")
            if entity_fsns is not None:
                if not entity_fsns:
                    return []
                fk_qs = _list_or_scalar_filter(fk_qs, "fsn", entity_fsns)
            return list(
                fk_qs.exclude(sku__isnull=True)
                .exclude(sku="")
                .values("sku")
                .annotate(total_rev=Sum("revenue"))
                .order_by("-total_rev")[:report_limit]
            )
        return []

    with ThreadPoolExecutor(max_workers=2) as executor:
        future_az_totals = executor.submit(fetch_az_totals)
        future_fk_totals = executor.submit(fetch_fk_totals)
        az_totals = future_az_totals.result()
        fk_totals = future_fk_totals.result()

    for t in az_totals:
        combined_totals.append({"sku": t["sku"], "rev": t["total_rev"] or 0, "plat": "Amazon"})
    for t in fk_totals:
        combined_totals.append({"sku": t["sku"], "rev": t["total_rev"] or 0, "plat": "Flipkart"})

    combined_totals.sort(key=lambda x: x["rev"], reverse=True)
    combined_totals = combined_totals[:report_limit]

    top_az_skus = [x["sku"] for x in combined_totals if x["plat"] == "Amazon"]
    top_fk_skus = [x["sku"] for x in combined_totals if x["plat"] == "Flipkart"]

    def fetch_az_rows_and_skus():
        if not top_az_skus:
            return [], {}
        az_qs = qs.filter(platform="Amazon")
        if entity_asins is not None:
            if not entity_asins:
                return [], {}
            az_qs = _list_or_scalar_filter(az_qs, "asin", entity_asins)
        rows = list(
            az_qs.filter(sku__in=top_az_skus)
            .values("sku", "date")
            .annotate(
                pageviews=Sum("page_views"),
                units=Sum("units_sold"),
                revenue=Sum("revenue"),
                ad_spend=Sum("ad_spend"),
            )
            .order_by("-date", "-revenue", "-units", "-pageviews", "sku")
        )
        skus = {
            r["asin"]: r["msku"] or ""
            for r in CategoryMapping.objects.filter(
                user=data_owner,
                asin__in=[r.get("sku") for r in rows if r.get("sku")],
            ).values("asin", "msku")
        }
        return rows, skus

    def fetch_fk_rows_and_skus():
        if not top_fk_skus:
            return [], {}
        fk_qs = qs.filter(platform="Flipkart")
        if entity_fsns is not None:
            if not entity_fsns:
                return [], {}
            fk_qs = _list_or_scalar_filter(fk_qs, "fsn", entity_fsns)
        rows = list(
            fk_qs.filter(sku__in=top_fk_skus)
            .values("sku", "date")
            .annotate(
                pageviews=Sum("page_views"),
                units=Sum("units_sold"),
                revenue=Sum("revenue"),
                ad_spend=Sum("ad_spend"),
            )
            .order_by("-date", "-revenue", "-units", "-pageviews", "sku")
        )
        skus = {
            r["fsn"]: r["sku"] or ""
            for r in FlipkartCategoryMap.objects.filter(
                user=data_owner,
                fsn__in=[r.get("sku") for r in rows if r.get("sku")],
            ).values("fsn", "sku")
        }
        return rows, skus

    with ThreadPoolExecutor(max_workers=2) as executor:
        future_az_rows = executor.submit(fetch_az_rows_and_skus)
        future_fk_rows = executor.submit(fetch_fk_rows_and_skus)
        asin_rows, asin_skus = future_az_rows.result()
        fsn_rows, fsn_skus = future_fk_rows.result()

    if not asin_rows and not fsn_rows:
        return None

    rows = [
        {
            "date": row.get("date"),
            "product_id": row.get("sku") or "",
            "sku": asin_skus.get(row.get("sku"), ""),
            "platform": "Amazon",
            "pageviews": int(row.get("pageviews") or 0),
            "units": int(row.get("units") or 0),
            "revenue": float(row.get("revenue") or 0),
            "ad_spend": float(row.get("ad_spend") or 0),
        }
        for row in asin_rows
    ]
    rows.extend(
        {
            "date": row.get("date"),
            "product_id": row.get("sku") or "",
            "sku": fsn_skus.get(row.get("sku"), ""),
            "platform": "Flipkart",
            "pageviews": int(row.get("pageviews") or 0),
            "units": int(row.get("units") or 0),
            "revenue": float(row.get("revenue") or 0),
            "ad_spend": float(row.get("ad_spend") or 0),
        }
        for row in fsn_rows
    )
    rows.sort(
        key=lambda row: (
            row.get("date") or datetime.date.min,
            float(row.get("revenue") or 0),
            int(row.get("units") or 0),
            int(row.get("pageviews") or 0),
        ),
        reverse=True,
    )
    return rows


def _get_asin_fsn_report_rows(data_owner, filters, report_date_range, report_limit, report_start_date=None, report_end_date=None):
    report_limit = _parse_positive_int(
        report_limit, default=10, minimum=1, maximum=100
    )
    product_daily_rows = _get_asin_fsn_report_product_daily_rows(
        data_owner, filters, report_date_range, report_limit, report_start_date, report_end_date
    )
    if product_daily_rows is not None:
        return product_daily_rows

    qs, fk_qs = _get_filtered_processed_querysets(data_owner, filters)
    qs = _apply_asin_fsn_report_date_range(qs, report_date_range, report_start_date, report_end_date)
    fk_qs = _apply_asin_fsn_report_date_range(fk_qs, report_date_range, report_start_date, report_end_date)

    asin_rows = []
    fsn_rows = []
    platform = filters.get("platform") or "All"
    show_amazon = platform != "Flipkart"
    show_flipkart = platform != "Amazon"

    combined_totals = []

    if show_amazon:
        az_totals = list(
            qs.exclude(asin__isnull=True)
            .exclude(asin="")
            .values("asin")
            .annotate(total_rev=Sum("revenue"))
            .order_by("-total_rev")[:report_limit]
        )
        for t in az_totals:
            combined_totals.append({"sku": t["asin"], "rev": t["total_rev"] or 0, "plat": "Amazon"})

    if show_flipkart:
        fk_totals = list(
            fk_qs.exclude(fsn__isnull=True)
            .exclude(fsn="")
            .values("fsn")
            .annotate(total_rev=Sum("revenue"))
            .order_by("-total_rev")[:report_limit]
        )
        for t in fk_totals:
            combined_totals.append({"sku": t["fsn"], "rev": t["total_rev"] or 0, "plat": "Flipkart"})

    combined_totals.sort(key=lambda x: x["rev"], reverse=True)
    combined_totals = combined_totals[:report_limit]

    top_az_skus = [x["sku"] for x in combined_totals if x["plat"] == "Amazon"]
    top_fk_skus = [x["sku"] for x in combined_totals if x["plat"] == "Flipkart"]

    if top_az_skus:
        asin_rows = list(
            qs.filter(asin__in=top_az_skus)
            .values("asin", "date")
            .annotate(
                pageviews=Sum("pageviews"),
                units=Sum("units"),
                revenue=Sum("revenue"),
                ad_spend=Sum("total_spend"),
            )
            .order_by("-date", "-revenue", "-units", "-pageviews", "asin")
        )

    if top_fk_skus:
        fsn_rows = list(
            fk_qs.filter(fsn__in=top_fk_skus)
            .values("fsn", "date")
            .annotate(
                pageviews=Sum("pageviews"),
                units=Sum("units"),
                revenue=Sum("revenue"),
                ad_spend=Sum("total_spend"),
            )
            .order_by("-date", "-revenue", "-units", "-pageviews", "fsn")
        )

    asin_skus = {
        row["asin"]: row["msku"] or ""
        for row in CategoryMapping.objects.filter(
            user=data_owner,
            asin__in=[row.get("asin") for row in asin_rows if row.get("asin")],
        ).values("asin", "msku")
    }
    fsn_skus = {
        row["fsn"]: row["sku"] or ""
        for row in FlipkartCategoryMap.objects.filter(
            user=data_owner,
            fsn__in=[row.get("fsn") for row in fsn_rows if row.get("fsn")],
        ).values("fsn", "sku")
    }

    rows = [
        {
            "date": row.get("date"),
            "product_id": row.get("asin") or "",
            "sku": asin_skus.get(row.get("asin"), ""),
            "platform": "Amazon",
            "pageviews": int(row.get("pageviews") or 0),
            "units": int(row.get("units") or 0),
            "revenue": float(row.get("revenue") or 0),
            "ad_spend": float(row.get("ad_spend") or 0),
        }
        for row in asin_rows
    ]
    rows.extend(
        {
            "date": row.get("date"),
            "product_id": row.get("fsn") or "",
            "sku": fsn_skus.get(row.get("fsn"), ""),
            "platform": "Flipkart",
            "pageviews": int(row.get("pageviews") or 0),
            "units": int(row.get("units") or 0),
            "revenue": float(row.get("revenue") or 0),
            "ad_spend": float(row.get("ad_spend") or 0),
        }
        for row in fsn_rows
    )
    rows.sort(
        key=lambda row: (
            row.get("date") or datetime.date.min,
            float(row.get("revenue") or 0),
            int(row.get("units") or 0),
            int(row.get("pageviews") or 0),
        ),
        reverse=True,
    )
    return rows


def _get_light_filter_metadata(data_owner_id, data_version):
    """
    Main dashboard payloads do not need full dropdown option lists.
    Remote Select2 endpoints provide paginated options on demand.
    """
    cache_key = f"dashboard_light_filter_metadata_{data_owner_id}_{data_version}"
    metadata = cache.get(cache_key)
    if metadata:
        return metadata

    platforms = []
    if ProcessedDashboardData.objects.filter(user_id=data_owner_id).exists():
        platforms.append("Amazon")
    if FlipkartProcessedDashboardData.objects.filter(user_id=data_owner_id).exists():
        platforms.append("Flipkart")

    # Collect distinct years from both Amazon and Flipkart processed data
    from django.db.models import Min, Max
    az_bounds = ProcessedDashboardData.objects.filter(user_id=data_owner_id).aggregate(min_d=Min("date"), max_d=Max("date"))
    fk_bounds = FlipkartProcessedDashboardData.objects.filter(user_id=data_owner_id).aggregate(min_d=Min("date"), max_d=Max("date"))

    years = set()
    for bounds in (az_bounds, fk_bounds):
        min_d = bounds.get("min_d")
        max_d = bounds.get("max_d")
        if min_d and max_d:
            years.update(range(min_d.year, max_d.year + 1))
    
    years = sorted(list(years), reverse=True)

    metadata = {
        "asins": [],
        "fsns": [],
        "categories": [],
        "portfolios": [],
        "subcategories": [],
        "category_managers": [],
        "series_names": [],
        "materials": [],
        "sizes": [],
        "brand_names": [],
        "ratings": [],
        "parent_asins": [],
        "finishes": [],
        "platforms": platforms,
        "years": years,
        "dates": [],
    }
    cache.set(cache_key, metadata, timeout=3600)
    return metadata



def _get_top_product_modal_rows(data_owner, filters):
    from apps.dashboard.services.analytics_services_orm_pipeline import (
        apply_global_filters_orm,
        _build_top_product_rows,
        _get_product_daily_summary_querysets,
        get_prev_period_qs,
        product_insights_need_exact_dates,
    )

    qs, fk_qs = _get_filtered_processed_querysets(data_owner, filters)
    qs_f = apply_global_filters_orm(qs, filters)
    fk_qs_f = apply_global_filters_orm(fk_qs, filters)
    qs_prev = get_prev_period_qs(qs, filters)
    fk_prev = get_prev_period_qs(fk_qs, filters)
    summary_az_base, summary_fk_base = _get_product_daily_summary_querysets(
        data_owner, filters, apply_date_filter=False
    )
    summary_az_f = apply_global_filters_orm(summary_az_base, filters)
    summary_fk_f = apply_global_filters_orm(summary_fk_base, filters)
    asin_meta = {
        row["asin"]: {
            "portfolio": row["portfolio"] or "",
            "msku": row["msku"] or "",
        }
        for row in CategoryMapping.objects.filter(user=data_owner).values("asin", "portfolio", "msku")
    }
    fsn_meta = {
        row["fsn"]: {
            "portfolio": row["portfolio"] or "",
            "sku": row["sku"] or "",
        }
        for row in FlipkartCategoryMap.objects.filter(user=data_owner).values("fsn", "portfolio", "sku")
    }

    try:
        from apps.dashboard.services.asin_monthly_summary import build_top_products_from_monthly

        if not product_insights_need_exact_dates(filters):
            monthly_rows = build_top_products_from_monthly(
                data_owner,
                filters,
                asin_meta=asin_meta,
                fsn_meta=fsn_meta,
                limit=200,
                include_full_payload=True,
            )
            if monthly_rows is not None:
                return monthly_rows
    except Exception:
        pass

    return _build_top_product_rows(
        qs_f,
        fk_qs_f,
        qs_prev,
        fk_prev,
        asin_meta=asin_meta,
        fsn_meta=fsn_meta,
        include_full_payload=True,
        summary_qs_f=summary_az_f,
        fk_summary_qs_f=summary_fk_f,
        summary_prev_f=get_prev_period_qs(summary_az_base, filters),
        fk_summary_prev_f=get_prev_period_qs(summary_fk_base, filters),
    )


def _get_declining_product_modal_rows(data_owner, filters):
    from apps.dashboard.services.analytics_services_orm_pipeline import (
        _build_declining_product_rows,
        _get_product_daily_summary_querysets,
        product_insights_need_exact_dates,
        resolve_growth_period,
        safe_shift_month,
    )

    qs, fk_qs = _get_filtered_processed_querysets(data_owner, filters)
    max_az = qs.aggregate(m=Max("date"))
    max_fk = fk_qs.aggregate(m=Max("date"))
    latest_dates = [item.get("m") for item in (max_az, max_fk) if item.get("m")]
    has_explicit_period = bool(
        filters.get("date_range") or filters.get("start_date") or filters.get("end_date")
    )
    if has_explicit_period:
        reference_date = timezone.localdate()
    else:
        reference_date = max(latest_dates) if latest_dates else datetime.date.today()
    cm_start, cm_end = resolve_growth_period(filters, reference_date)
    pm_start = safe_shift_month(cm_start, -1)
    pm_end = safe_shift_month(cm_end, -1)

    # Build meta dicts with SKU data for the msku column
    asin_meta = {
        row["asin"]: {
            "portfolio": row["portfolio"] or "",
            "msku": row["msku"] or "",
        }
        for row in CategoryMapping.objects.filter(user=data_owner).values("asin", "portfolio", "msku")
    }
    fsn_meta = {
        row["fsn"]: {
            "portfolio": row["portfolio"] or "",
            "sku": row["sku"] or "",
        }
        for row in FlipkartCategoryMap.objects.filter(user=data_owner).values("fsn", "portfolio", "sku")
    }

    try:
        from apps.dashboard.services.asin_monthly_summary import build_declining_products_from_monthly

        if not product_insights_need_exact_dates(filters):
            monthly_rows = build_declining_products_from_monthly(
                data_owner,
                filters,
                cm_start,
                cm_end,
                pm_start,
                pm_end,
                include_full_payload=True,
                asin_meta=asin_meta,
                fsn_meta=fsn_meta,
            )
            if monthly_rows is not None:
                return monthly_rows
    except Exception:
        pass

    summary_az_base, summary_fk_base = _get_product_daily_summary_querysets(
        data_owner, filters, apply_date_filter=False
    )
    return _build_declining_product_rows(
        qs,
        fk_qs,
        cm_start,
        cm_end,
        pm_start,
        pm_end,
        include_full_payload=True,
        asin_meta=asin_meta,
        fsn_meta=fsn_meta,
        summary_qs=summary_az_base,
        fk_summary_qs=summary_fk_base,
    )


def _get_inventory_modal_queryset(data_owner, filters, query):
    from apps.dashboard.services.analytics_services_orm_pipeline import (
        apply_inventory_summary_filters,
    )

    qs = DashboardInventoryHealthSummary.objects.filter(
        user=data_owner,
        platform="Combined",
    )
    platform_filter = filters.get("platform")
    qs = apply_inventory_summary_filters(qs, data_owner, filters, platform_filter)

    if query:
        needle = str(query).strip()
        qs = qs.filter(
            Q(sku__icontains=needle)
            | Q(asin__icontains=needle)
            | Q(fsn__icontains=needle)
            | Q(category__icontains=needle)
            | Q(portfolio__icontains=needle)
            | Q(subcategory__icontains=needle)
            | Q(status__icontains=needle)
            | Q(fk_status__icontains=needle)
            | Q(reason__icontains=needle)
        )
    return qs.order_by("-date", "-revenue", "sku")


def _get_npd_modal_rows(data_owner, filters):
    from apps.dashboard.services.analytics_services_orm_pipeline import apply_global_filters_orm
    from apps.dashboard.services.npd import build_npd_performance

    qs, fk_qs = _get_filtered_processed_querysets(data_owner, filters)
    qs_f = apply_global_filters_orm(qs, filters)
    fk_qs_f = apply_global_filters_orm(fk_qs, filters)
    return (build_npd_performance(data_owner, filters, qs_f, fk_qs_f, include_trend=False).get("rows") or [])


def _inventory_summary_row_dict(row, msku_map=None):
    _msku_map = msku_map or {}
    # For Amazon: asin→msku; for Flipkart: fsn→sku; Combined: check both
    _asin = row.asin or ""
    _fsn = row.fsn or ""
    _msku = _msku_map.get(_asin) or _msku_map.get(_fsn) or ""
    amz_fba = int(row.fba_qty or 0)
    amz_flex = int(row.flex_qty or 0)
    total_amz_stock = int(row.stock_qty or 0)
    fk_fbf = int(row.fk_fba_qty or 0)
    fk_flex = int(row.fk_flex_qty or 0)
    total_fk_stock = int(row.fk_stock_qty or 0)
    total_stock = total_amz_stock + total_fk_stock
    amz_sales = int(row.sale_qty or 0)
    fk_sales = int(row.fk_sale_qty or 0)
    total_sales = amz_sales + fk_sales
    total_doc = round(total_stock / float(total_sales), 2) if total_sales > 0 else (999.0 if total_stock > 0 else 0.0)
    total_reason = (
        f"DOC = ∞ (Total Stock: {total_stock}, No sales)"
        if total_stock > 0 and total_sales <= 0
        else (
            f"Total Stock = 0 (AMZ: {total_amz_stock}, FK: {total_fk_stock})"
            if total_stock <= 0
            else f"DOC = {total_doc} days (Total Stock: {total_stock} / Total Same-Day Sales: {total_sales})"
        )
    )
    return {
        "date": row.date,
        "sku": row.sku,
        "msku": _msku,
        "asin": _asin,
        "fsn": _fsn,
        "category": row.category or "Unknown",
        "portfolio": row.portfolio or "",
        
        # Amazon Metrics
        "amz_fba": amz_fba,
        "amz_flex": amz_flex,
        "amz_stock": total_amz_stock,
        "total_amz_stock": total_amz_stock,
        "amz_sales": amz_sales,
        "amz_doc": float(row.doc or 0),
        "amz_revenue": round(float(row.revenue or 0), 2),
        "amz_status": row.status or "",
        "amz_status_class": row.status_class or "",
        
        # Flipkart Metrics
        "fk_fbf": fk_fbf,
        "fk_flex": fk_flex,
        "fk_stock": total_fk_stock,
        "total_fk_stock": total_fk_stock,
        "fk_sales": fk_sales,
        "fk_doc": float(row.fk_doc or 0),
        "fk_revenue": round(float(row.fk_revenue or 0), 2),
        "fk_status": row.fk_status or "",
        "fk_status_class": row.fk_status_class or "",

        # Combined stock/DOC metrics
        "total_stock": total_stock,
        "total_sales": total_sales,
        "total_doc": total_doc,
        "total_reason": total_reason,
        
        # Calculate fk_reason dynamically since only amz_reason is stored in DB
        "fk_reason": (
            f"Stock Qty = 0 (FBF: {fk_fbf}, Flex: {fk_flex})"
            if total_fk_stock <= 0
            else (
                f"DOC = ∞ (Stock: {total_fk_stock}, No sales)"
                if fk_sales <= 0
                else f"DOC = {round(float(row.fk_doc or 0), 2)} days (Stock: {total_fk_stock} / Same-Day Sales: {fk_sales})"
            )
        ),
        
        # Combined / Legacy (for sorting/fallback)
        "revenue": round(float(row.revenue or 0) + float(row.fk_revenue or 0), 2),
        "stock_qty": total_stock,
        "sale_qty": total_sales,
        "reason": row.reason,
    }


def _build_template_payload(payload):
    """
    Keep template payload separate from cached payload mutation.
    """
    return deepcopy(payload) if isinstance(payload, dict) else payload


def _trim_payload_for_initial_load(payload):
    """
    Keep initial section payload lightweight; large modal datasets are loaded on demand.
    """
    if not isinstance(payload, dict):
        return payload
    payload = deepcopy(payload)
    payload["cat_all_top_products"] = []
    payload["cat_all_under_products"] = []
    payload["npd_products_all"] = []
    if isinstance(payload.get("category_performance"), list):
        payload["category_performance"] = payload["category_performance"][:25]
    if isinstance(payload.get("cluster_performance"), list):
        payload["cluster_performance"] = payload["cluster_performance"][:25]
    forecast = payload.get("forecast")
    if isinstance(forecast, dict) and isinstance(forecast.get("details"), list):
        forecast["details"] = forecast["details"][:31]
    inventory = payload.get("inventory")
    if isinstance(inventory, dict):
        inventory["details"] = []
        inventory["details_shown"] = 0
        inventory["details_truncated"] = False
    return payload


def _get_dashboard_refresh_status(data_owner_id):
    cache_key = f"dashboard_refresh_status_{data_owner_id}"
    status = cache.get(cache_key)
    if not isinstance(status, dict):
        return {"state": "idle", "message": ""}
    state = str(status.get("state") or "idle").lower()
    if state not in {"idle", "processing", "success", "error"}:
        state = "idle"
    message = str(status.get("message") or "")
    now = time.time()

    if state == "processing":
        # Guard against stale "processing" banners when no real work remains.
        # Three independent signals indicate genuine activity:
        #   1. A refresh lock held by an active Celery task
        #   2. A recent cache "ping" from _set_dashboard_refresh_status (≤120s)
        #   3. Recently-updated UploadLog entries in QUEUED/PROCESSING state (≤30m)
        # If ALL three are absent/stale, the banner is a leftover from a
        # crashed/killed worker and should be reset to idle.

        # -- Signal 1: Refresh lock --
        lock_key = f"dashboard_refresh_lock_{data_owner_id}"
        lock_ts_key = f"{lock_key}_ts"
        has_refresh_lock = bool(cache.get(lock_key))

        # Detect stale locks from crashed workers. The lock itself has a
        # 1800s Redis TTL, but the worker may have died without cleanup.
        # If the lock's timestamp is missing or older than 15 minutes,
        # treat it as abandoned and clear it.
        if has_refresh_lock:
            lock_ts = cache.get(lock_ts_key)
            lock_is_stale = True
            if lock_ts:
                try:
                    lock_age = now - float(lock_ts)
                    lock_is_stale = lock_age > 900  # 15 minutes
                except (ValueError, TypeError):
                    lock_is_stale = True
            if lock_is_stale:
                cache.delete(lock_key)
                cache.delete(lock_ts_key)
                has_refresh_lock = False

        # -- Signal 2: Recent processing ping --
        has_recent_processing_ping = False
        ts = status.get("updated_at_ts")
        if isinstance(ts, (int, float)):
            has_recent_processing_ping = (now - float(ts)) <= 120

        # -- Signal 3: Active UploadLog entries --
        # Only hit the DB if Signals 1 and 2 haven't already confirmed activity.
        # Results are cached 15s to absorb the 3s polling cadence.
        has_active_upload_logs = False
        if not (has_refresh_lock or has_recent_processing_ping):
            _ul_key = f"dashboard_upload_log_active_{data_owner_id}"
            _ul_cached = cache.get(_ul_key)
            if _ul_cached is None:
                try:
                    from apps.upload.models import UploadLog
                    stale_cutoff = datetime.datetime.now() - datetime.timedelta(minutes=30)
                    has_active_upload_logs = UploadLog.objects.filter(
                        data_owner_id=data_owner_id,
                        status__in=[
                            UploadLog.STATUS_QUEUED,
                            UploadLog.STATUS_PROCESSING,
                        ],
                        updated_at__gte=stale_cutoff,
                    ).exists()
                    cache.set(_ul_key, has_active_upload_logs, timeout=15)
                    # Auto-cleanup: throttled to once per 60s to avoid write storms.
                    _cleanup_key = f"dashboard_upload_log_cleanup_{data_owner_id}"
                    if not cache.get(_cleanup_key):
                        cache.set(_cleanup_key, 1, timeout=60)
                        UploadLog.objects.filter(
                            data_owner_id=data_owner_id,
                            status__in=[
                                UploadLog.STATUS_QUEUED,
                                UploadLog.STATUS_PROCESSING,
                            ],
                            updated_at__lt=stale_cutoff,
                        ).update(
                            status=UploadLog.STATUS_ERROR,
                            message="Automatically marked as failed — task did not complete within 30 minutes.",
                        )
                except Exception:
                    has_active_upload_logs = False
            else:
                has_active_upload_logs = _ul_cached

        if not (has_refresh_lock or has_active_upload_logs or has_recent_processing_ping):
            cache.set(
                cache_key,
                {"state": "idle", "message": "", "updated_at_ts": now},
                timeout=300,
            )
            return {"state": "idle", "message": ""}

    # Prevent stale terminal banners from persisting across page refreshes.
    # Keep only recent success/error updates visible.
    if state in {"success", "error"}:
        ts = status.get("updated_at_ts")
        # Backward compatibility: older cache entries without timestamp are stale.
        is_stale = (not isinstance(ts, (int, float))) or ((now - float(ts)) > 45)
        if is_stale:
            cache.set(
                cache_key,
                {"state": "idle", "message": "", "updated_at_ts": now},
                timeout=300,
            )
            return {"state": "idle", "message": ""}
    return {"state": state, "message": message}


def _payload_needs_refresh(payload):
    """
    Detect stale cached payloads from older schema versions that can
    cause oversized HTML responses and outdated calculations.
    """
    if not isinstance(payload, dict):
        return True

    inventory = payload.get("inventory")
    if not isinstance(inventory, dict):
        return True

    required_inventory_keys = {
        "details_total",
        "details_shown",
        "details_truncated",
        "has_stock_data",
        "num_sale_days",
    }
    if not required_inventory_keys.issubset(inventory.keys()):
        return True

    return False


def _is_kpis_only_payload(payload):
    """
    Detect KPI-only payloads so analytics sections do not reuse them from
    materialized summaries.
    """
    if not isinstance(payload, dict):
        return True
    scope = str(payload.get("_compute_scope") or "").lower()
    if scope == "kpis":
        return True
    if scope == "full":
        return False
    # Backward-compatible heuristic for pre-marker payloads.
    charts = payload.get("charts")
    if isinstance(charts, dict) and not charts:
        forecast = payload.get("forecast") or {}
        if not payload.get("category_performance") and not payload.get("cluster_performance"):
            if int(forecast.get("days_in_month") or 0) == 0:
                return True
    return False


def no_cache_for_htmx(view_func):
    """Decorator to prevent caching of HTMX requests"""

    def wrapper(request, *args, **kwargs):
        response = view_func(request, *args, **kwargs)

        # Set no-cache headers for HTMX requests
        if request.headers.get("HX-Request") == "true":
            response["Cache-Control"] = "no-cache, no-store, must-revalidate, max-age=0"
            response["Pragma"] = "no-cache"
            response["Expires"] = "0"

        return response

    return wrapper


def dashboard_view(request):
    # Redirect the user to the first dashboard they have access to.
    user = get_logged_in_user(request)
    if not user:
        return redirect("account-login")
    route = _first_allowed_dashboard_for(user)
    return redirect(route)


def _ensure_processed_tables_if_missing(data_owner):
    """
    Self-heal when processed dashboard tables are empty but raw upload tables exist.
    This guards against edge cases where upload completion succeeded but the final
    processed-table build was skipped/interrupted.
    """
    refresh_status = _get_dashboard_refresh_status(data_owner.id)
    if refresh_status.get("state") == "processing":
        return

    presence_key = f"processed_tables_present_{data_owner.id}"
    if cache.get(presence_key):
        return

    has_amz_processed = ProcessedDashboardData.objects.filter(user=data_owner).exists()
    has_fk_processed = FlipkartProcessedDashboardData.objects.filter(user=data_owner).exists()

    if has_amz_processed or has_fk_processed:
        cache.set(presence_key, True, timeout=300)
        return

    has_amz_raw = (
        SalesData.objects.filter(user=data_owner).exists()
        or SpendData.objects.filter(user=data_owner).exists()
    )
    has_fk_raw = (
        FlipkartSearchTraffic.objects.filter(user=data_owner).exists()
        or FlipkartPLA.objects.filter(user=data_owner).exists()
    )

    if not has_amz_raw and not has_fk_raw:
        return

    from apps.upload.dashboard_builders import (
        generate_dashboard_data,
        generate_flipkart_dashboard_data,
    )

    if has_amz_raw:
        generate_dashboard_data(data_owner)
    if has_fk_raw:
        generate_flipkart_dashboard_data(data_owner)


def get_dashboard_context(
    request,
    include_payload=True,
    cache_view_type=None,
    include_full_payload=False,
    section_scope="all",
    compute_scope="full",
):
    user = get_logged_in_user(request)
    if not user:
        return None

    data_owner = user.created_by if user.created_by else user

    if user.is_main_user:
        _feat_key = "all_feature_codenames_v1"
        user_features = cache.get(_feat_key)
        if user_features is None:
            user_features = list(Feature.objects.values_list("code_name", flat=True))
            cache.set(_feat_key, user_features, timeout=3600)
    else:
        if user.role:
            _feat_key = f"role_feature_codenames_v1_{user.role_id}"
            user_features = cache.get(_feat_key)
            if user_features is None:
                user_features = list(
                    user.role.features.values_list("code_name", flat=True)
                )
                cache.set(_feat_key, user_features, timeout=3600)
        else:
            user_features = []

    filters = build_filters_from_querydict(request.GET)
    filters.pop("scope", None)
    selected_filters = selected_filter_payload(filters)

    data_version_dates = cache.get(f"dashboard_data_version_{data_owner.id}", 0)
    uploaded_dates_key = f"dashboard_uploaded_dates_v3_{data_owner.id}_{data_version_dates}"
    pending_dates_key = f"dashboard_pending_dates_v3_{data_owner.id}_{data_version_dates}"
    uploaded_dates = cache.get(uploaded_dates_key)
    pending_dates = cache.get(pending_dates_key)
    if uploaded_dates is None or pending_dates is None:
        from concurrent.futures import ThreadPoolExecutor

        def fetch_az_dates():
            return set(DashboardDailySummary.objects.filter(user_id=data_owner.id, platform="Amazon").dates("date", "day"))

        def fetch_fk_dates():
            return set(DashboardDailySummary.objects.filter(user_id=data_owner.id, platform="Flipkart").dates("date", "day"))

        def fetch_raw_fk_dates():
            return set(FlipkartSearchTraffic.objects.filter(user_id=data_owner.id).dates("date", "day"))

        def fetch_raw_az_dates():
            return set(SalesData.objects.filter(user_id=data_owner.id).dates("date", "day"))

        with ThreadPoolExecutor(max_workers=4) as executor:
            f_az = executor.submit(fetch_az_dates)
            f_fk = executor.submit(fetch_fk_dates)
            f_raw_fk = executor.submit(fetch_raw_fk_dates)
            f_raw_az = executor.submit(fetch_raw_az_dates)

            az_dates = f_az.result()
            fk_dates = f_fk.result()
            raw_fk_dates = f_raw_fk.result()
            raw_az_dates = f_raw_az.result()

        all_processed_dates = az_dates | fk_dates
        all_raw_dates = raw_fk_dates | raw_az_dates
        pending_only_dates = all_raw_dates - all_processed_dates

        # Blue tick dates = processed data ready (union of all sources for selectability)
        all_dts = sorted(list(all_processed_dates | all_raw_dates))
        uploaded_dates = [d.strftime("%Y-%m-%d") for d in all_dts if d]
        pending_dates = sorted([d.strftime("%Y-%m-%d") for d in pending_only_dates if d])
        cache.set(uploaded_dates_key, uploaded_dates, timeout=86400)
        cache.set(pending_dates_key, pending_dates, timeout=86400)

    if not include_payload:
        refresh_status = _get_dashboard_refresh_status(data_owner.id)
        return {
            "logged_user": user,
            "user_features": user_features,
            "payload": None,
            "payload_json": "null",
            "filters": filters,
            "selected_filters": selected_filters,
            "selected_filters_json": json.dumps(selected_filters),
            "dashboard_refresh_status": refresh_status,
            "dashboard_refresh_status_json": json.dumps(refresh_status),
            "uploaded_dates_json": json.dumps(uploaded_dates),
            "pending_dates_json": json.dumps(pending_dates),
        }

    _ensure_processed_tables_if_missing(data_owner)
    qs = ProcessedDashboardData.objects.filter(user=data_owner)
    fk_qs = FlipkartProcessedDashboardData.objects.filter(user=data_owner)

    data_version = cache.get(f"dashboard_data_version_{data_owner.id}", 0)
    cached_filter_metadata = _get_light_filter_metadata(data_owner.id, data_version)
    filter_key_str = cache_filter_string(filters)
    cache_hash = hashlib.md5(filter_key_str.encode("utf-8")).hexdigest()

    qs, fk_qs = apply_dashboard_entity_filters(qs, fk_qs, filters, user=data_owner)

    presence_cache_key = (
        f"dashboard_presence_v1_{data_owner.id}_{data_version}_{cache_hash}"
    )
    has_filtered_rows = cache.get(presence_cache_key)
    if has_filtered_rows is None:
        has_filtered_rows = qs.exists() or fk_qs.exists()
        cache.set(presence_cache_key, has_filtered_rows, timeout=300)

    if not has_filtered_rows:
        refresh_status = _get_dashboard_refresh_status(data_owner.id)
        return {
            "logged_user": user,
            "user_features": user_features,
            "payload": None,
            "payload_json": "null",
            "filters": filters,
            "selected_filters": selected_filters,
            "selected_filters_json": json.dumps(selected_filters),
            "dashboard_refresh_status": refresh_status,
            "dashboard_refresh_status_json": json.dumps(refresh_status),
            "uploaded_dates_json": json.dumps(uploaded_dates),
            "pending_dates_json": json.dumps(pending_dates),
        }

    # Apply same entity filters to spend data at DB level
    spend_qs = SpendData.objects.filter(user=data_owner)
    asin_filter = filters.get("asin")
    if asin_filter:
        if isinstance(asin_filter, (list, tuple)):
            spend_qs = spend_qs.filter(asin__in=asin_filter)
        else:
            spend_qs = spend_qs.filter(asin=asin_filter)
    if (
        filters.get("category")
        or filters.get("portfolio")
        or filters.get("subcategory")
        or filters.get("category_manager")
        or filters.get("series_name")
        or filters.get("material")
        or filters.get("size")
        or filters.get("brand_name")
        or filters.get("ratings")
        or filters.get("parent_asin")
        or filters.get("finish")
        or filters.get("launch_date_range")
        or filters.get("launch_start_date")
        or filters.get("launch_end_date")
    ):
        from apps.dashboard.services.filters import get_filtered_mapping_querysets
        az_map_qs, _ = get_filtered_mapping_querysets(filters, user=data_owner)
        spend_qs = spend_qs.filter(asin__in=az_map_qs.values("asin"))

    from apps.dashboard.services.analytics_services_orm_pipeline import run_orm_computation

    # Normalize filters once; reuse in memory cache + materialized summary table.
    view_type = cache_view_type or request.resolver_match.url_name or "shared"
    if view_type in ["business-dashboard", "category-dashboard", "ceo-dashboard"]:
        view_type = "ceo-dashboard"
    cache_mode = "full" if include_full_payload else "lite"
    cache_key = (
        f"dashboard_payload_v{DASHBOARD_PAYLOAD_CACHE_VERSION}_"
        f"s{DASHBOARD_CACHE_SCHEMA_VERSION}_"
        f"{data_owner.id}_{view_type}_{section_scope}_{data_version}_{cache_hash}_{cache_mode}"
    )

    payload = cache.get(cache_key)
    if payload and _payload_needs_refresh(payload):
        payload = None

    if not payload and not include_full_payload:
        payload = get_materialized_summary(
            user_id=data_owner.id,
            view_type=view_type,
            data_version=data_version,
            filter_hash=cache_hash,
        )
        if payload and _payload_needs_refresh(payload):
            payload = None
        elif str(compute_scope or "full").lower() == "full" and _is_kpis_only_payload(payload):
            payload = None

    if not payload:
        # For full-scope sections (visuals/details) share the lock across sections so
        # parallel page loads don't run the same heavy queries twice simultaneously.
        is_shared_full_lock = not include_full_payload and str(compute_scope or "full").lower() == "full"
        if is_shared_full_lock:
            compute_lock_key = (
                f"dashboard_compute_lock_v{DASHBOARD_PAYLOAD_CACHE_VERSION}_"
                f"{data_owner.id}_{view_type}_{data_version}_{cache_hash}"
            )
        else:
            compute_lock_key = f"{cache_key}:lock"
        have_lock = cache.add(compute_lock_key, "1", timeout=300)
        if not have_lock:
            # Another section is computing the same dataset; wait and reuse.
            # With monthly summary the typical computation is 1–3 s, so reduce
            # wait_sleep to 0.5 s and check materialized summary every 4 iterations
            for _wi in range(400):
                time.sleep(0.5)
                payload = cache.get(cache_key)
                if payload:
                    break
                # Check shared materialized summary every 4 iterations (every 2 s)
                # to avoid N×DashboardMaterializedSummary queries per waiting section.
                if not payload and is_shared_full_lock and (_wi % 4 == 0):
                    payload = get_materialized_summary(
                        user_id=data_owner.id,
                        view_type=view_type,
                        data_version=data_version,
                        filter_hash=cache_hash,
                    )
                    if payload and _payload_needs_refresh(payload):
                        payload = None
                    elif payload and _is_kpis_only_payload(payload):
                        payload = None
                if payload:
                    break
        if not payload:
            try:
                payload = run_orm_computation(
                    qs,
                    fk_qs,
                    spend_qs,
                    filters,
                    data_owner,
                    cached_filter_metadata=cached_filter_metadata,
                    include_full_payload=include_full_payload,
                    compute_scope=compute_scope,
                    cache_identity={
                        "data_version": data_version,
                        "filter_hash": cache_hash,
                    },
                    section_scope=section_scope,
                    dashboard_view=(request.resolver_match.kwargs.get("view_name") if request.resolver_match else None),
                )
                if (not include_full_payload) and str(compute_scope or "full").lower() == "full":
                    try:
                        store_materialized_summary(
                            user_id=data_owner.id,
                            view_type=view_type,
                            data_version=data_version,
                            filter_hash=cache_hash,
                            normalized_filters=json.dumps(
                                normalize_payload_filters(filters), sort_keys=True
                            ),
                            payload=payload,
                        )
                    except Exception:
                        # Materialized summaries are a performance layer; do not fail requests.
                        pass

                cache.set(
                    cache_key,
                    payload,
                    timeout=(
                        DASHBOARD_CACHE_TTL_FULL_SECONDS
                        if include_full_payload
                        else DASHBOARD_CACHE_TTL_LITE_SECONDS
                    ),
                )
            finally:
                if have_lock:
                    cache.delete(compute_lock_key)

    if not include_full_payload:
        payload = _trim_payload_for_initial_load(payload)

    template_payload = _build_template_payload(payload)
    refresh_status = _get_dashboard_refresh_status(data_owner.id)

    return {
        "logged_user": user,
        "user_features": user_features,
        "payload": template_payload,
        "payload_json": _build_payload_json(payload),
        "filters": filters,
        "selected_filters": selected_filters,
        "selected_filters_json": json.dumps(selected_filters),
        "dashboard_refresh_status": refresh_status,
        "dashboard_refresh_status_json": json.dumps(refresh_status),
        "uploaded_dates_json": json.dumps(uploaded_dates),
        "pending_dates_json": json.dumps(pending_dates),
    }


def _inject_htmx(request, ctx):
    """
    Inject base_template into context.
    Ensures base_template is ALWAYS set to prevent extends tag errors.
    """
    # Ensure ctx is always a dict (not None)
    if ctx is None:
        ctx = {
            "logged_user": None,
            "user_features": [],
            "payload": None,
            "payload_json": "null",
            "filters": {},
            "selected_filters": {},
            "selected_filters_json": "{}",
        }

    # Determine which base template to use
    is_htmx_request = request.headers.get("HX-Request") == "true"
    ctx["base_template"] = (
        "dashboard/base_htmx.html"
        if is_htmx_request
        else "dashboard/base_dashboard.html"
    )

    return ctx


# ─────────────────────────────────────────────────────────
# Dashboard views
# ─────────────────────────────────────────────────────────


@require_feature("business_dashboard")
@no_cache_for_htmx
def business_dashboard_view(request):
    ctx = get_dashboard_context(
        request,
        include_payload=False,
        cache_view_type="business-dashboard",
    )
    if ctx is None:
        return redirect("account-login")
    return render(
        request, "dashboard/business_dashboard.html", _inject_htmx(request, ctx)
    )


@require_feature("ceo_dashboard")
@no_cache_for_htmx
def ceo_dashboard_view(request):
    ctx = get_dashboard_context(
        request,
        include_payload=False,
        cache_view_type="ceo-dashboard",
    )
    if ctx is None:
        return redirect("account-login")
    return render(request, "dashboard/ceo_dashboard.html", _inject_htmx(request, ctx))


@require_feature("category_dashboard")
@no_cache_for_htmx
def category_dashboard_view(request):
    ctx = get_dashboard_context(
        request,
        include_payload=False,
        cache_view_type="category-dashboard",
    )
    if ctx is None:
        return redirect("account-login")
    return render(
        request, "dashboard/category_dashboard.html", _inject_htmx(request, ctx)
    )


def _user_has_feature(user, feature_code):
    if user.is_main_user:
        return True
    return bool(
        user.role and user.role.features.filter(code_name=feature_code).exists()
    )


@no_cache_for_htmx
def dashboard_section_view(request, view_name, section):
    user = get_logged_in_user(request)
    if not user:
        return JsonResponse({"error": "Not authenticated"}, status=401)

    feature_code = DASHBOARD_FEATURE_BY_VIEW.get(view_name)
    if not feature_code:
        return JsonResponse({"error": "Invalid dashboard view."}, status=404)
    if not _user_has_feature(user, feature_code):
        return JsonResponse({"error": "Permission denied."}, status=403)

    template_name = DASHBOARD_SECTION_TEMPLATE_MAP.get((view_name, section))
    if not template_name:
        return JsonResponse({"error": "Invalid section."}, status=404)

    compute_scope = (request.GET.get("scope") or "full").strip().lower()

    ctx = get_dashboard_context(
        request,
        include_payload=True,
        cache_view_type=f"{view_name}-dashboard",
        section_scope=section,
        compute_scope=compute_scope,
    )
    if ctx is None:
        return JsonResponse({"error": "Not authenticated"}, status=401)

    ctx["dashboard_section_view_type"] = view_name
    ctx["dashboard_section_name"] = section
    ctx["section_template"] = template_name
    return render(request, "dashboard/sections/section_wrapper.html", ctx)


@no_cache_for_htmx
def dashboard_modal_rows_view(request, view_name, modal_key):
    user = get_logged_in_user(request)
    if not user:
        return JsonResponse({"error": "Not authenticated"}, status=401)

    feature_code = DASHBOARD_FEATURE_BY_VIEW.get(view_name)
    if not feature_code:
        return JsonResponse({"error": "Invalid dashboard view."}, status=404)
    if not _user_has_feature(user, feature_code):
        return JsonResponse({"error": "Permission denied."}, status=403)

    modal_tpl = DASHBOARD_MODAL_ROWS_TEMPLATE_MAP.get((view_name, modal_key))
    if not modal_tpl:
        return JsonResponse({"error": "Invalid modal key."}, status=404)
    template_name, payload_key = modal_tpl

    export_format = (request.GET.get("export") or "").strip().lower()
    
    dt_draw = request.GET.get("draw")
    if dt_draw:
        query = (request.GET.get("search[value]") or "").strip().lower()
        dt_start = _parse_positive_int(request.GET.get("start"), default=0, minimum=0, maximum=10_000_000)
        dt_length = _parse_positive_int(request.GET.get("length"), default=25, minimum=10, maximum=200)
        page_size = dt_length
        page = (dt_start // page_size) + 1
    else:
        query = (request.GET.get("q") or "").strip().lower()
        page = _parse_positive_int(request.GET.get("page"), default=1, minimum=1, maximum=10_000)
        page_size = _parse_positive_int(
            request.GET.get("page_size"), default=50, minimum=10, maximum=200
        )

    data_owner = user.created_by if user.created_by else user
    data_version = cache.get(f"dashboard_data_version_{data_owner.id}", 0)
    filters = _strip_non_dashboard_filters(build_filters_from_querydict(request.GET))
    filter_hash = hashlib.md5(cache_filter_string(filters).encode("utf-8")).hexdigest()

    modal_rows_cache_key = (
        "dashboard_modal_rows_v6_"
        f"{data_owner.id}_{view_name}_{modal_key}_{data_version}_{filter_hash}_"
        f"{hashlib.md5(query.encode('utf-8')).hexdigest()}_{page}_{page_size}"
    )
    modal_rows_cache_ttl = int(
        getattr(settings, "DASHBOARD_MODAL_ROWS_CACHE_TTL_SECONDS", 180)
    )

    # DataTables mode: client handles all pagination/search — return all rows at once.
    load_all = request.GET.get("all") == "1"

    # Skip legacy per-page cache for DataTables server-side requests
    if export_format not in {"csv", "excel", "xlsx"} and not load_all and not dt_draw:
        cached_modal_payload = cache.get(modal_rows_cache_key)
        if cached_modal_payload:
            return JsonResponse(cached_modal_payload)


    # All-rows cache: keyed without page/page_size so page 2+ hits this and paginates in Python.
    all_rows_cache_key = (
        "dashboard_modal_all_rows_v7_"
        f"{data_owner.id}_{view_name}_{modal_key}_{data_version}_{filter_hash}_"
        f"{hashlib.md5(query.encode('utf-8')).hexdigest()}"
    )

    total = 0
    rows = None

    if modal_key == "inventory-health":
        inventory_qs = _get_inventory_modal_queryset(data_owner, filters, query)
        total = inventory_qs.count()
        row_qs = (
            inventory_qs
            if export_format in {"csv", "excel", "xlsx"}
            else inventory_qs[(page - 1) * page_size : page * page_size]
        )
        # Build ASIN→msku and FSN→sku lookup maps for the SKU column
        from apps.dashboard.models import CategoryMapping, FlipkartCategoryMap
        _inv_msku_map = {}
        for _r in CategoryMapping.objects.filter(user=data_owner).values("asin", "msku"):
            if _r["asin"] and _r["msku"]:
                _inv_msku_map[_r["asin"]] = _r["msku"]
        for _r in FlipkartCategoryMap.objects.filter(user=data_owner).values("fsn", "sku"):
            if _r["fsn"] and _r["sku"]:
                _inv_msku_map[_r["fsn"]] = _r["sku"]
        rows = [_inventory_summary_row_dict(row, msku_map=_inv_msku_map) for row in row_qs]
    else:
        # Fast path 1: per-page HTML already cached (only for non-DataTables mode)
        # (handled above via modal_rows_cache_key check)

        # Fast path 2: full rows already cached in memory
        if not export_format:
            rows = cache.get(all_rows_cache_key)

        # Fast path 3: reuse materialized summary computed by the section view.
        # Only for modals whose payload key is always fully populated in the materialized
        # payload — top-products and declining-products store empty lists there.
        MATERIALIZED_MODAL_KEYS = {"cluster-performance", "category-growth"}
        if rows is None and modal_key in MATERIALIZED_MODAL_KEYS:
            mat_payload = get_materialized_summary(
                user_id=data_owner.id,
                view_type=f"{view_name}-dashboard",
                data_version=data_version,
                filter_hash=filter_hash,
            )
            if mat_payload and not _payload_needs_refresh(mat_payload):
                mat_rows = _resolve_payload_key(mat_payload, payload_key)
                if isinstance(mat_rows, list) and mat_rows:
                    rows = _filter_rows_by_query(mat_rows, query)
                    if not export_format:
                        cache.set(all_rows_cache_key, rows, timeout=modal_rows_cache_ttl)

        # Slow path: compute from scratch
        if rows is None:
            if modal_key == "top-products":
                rows = _filter_rows_by_query(_get_top_product_modal_rows(data_owner, filters), query)
            elif modal_key == "npd-performance":
                rows = _filter_rows_by_query(_get_npd_modal_rows(data_owner, filters), query)
            elif modal_key == "declining-products":
                rows = _filter_rows_by_query(
                    _get_declining_product_modal_rows(data_owner, filters), query
                )
            else:
                ctx = get_dashboard_context(
                    request,
                    include_payload=True,
                    cache_view_type=f"{view_name}-dashboard",
                    section_scope="analytics",
                    compute_scope="full",
                )
                if ctx is None:
                    return JsonResponse({"error": "Not authenticated"}, status=401)
                payload = ctx.get("payload") or {}
                rows = _resolve_payload_key(payload, payload_key)
                if not isinstance(rows, list):
                    rows = []
                rows = _filter_rows_by_query(rows, query)

            if not export_format:
                cache.set(all_rows_cache_key, rows or [], timeout=modal_rows_cache_ttl)

        if rows is None:
            rows = []
        total = len(rows)

    if export_format in {"csv", "excel", "xlsx"}:
        # Platform-aware export for inventory health
        if modal_key == "inventory-health":
            _platform = filters.get("platform") or "All"
            if _platform == "Amazon":
                headers = ["Date", "ASIN", "Category", "Portfolio", "AMZ FBA", "AMZ Flex", "Total AMZ Stock", "AMZ Sales", "AMZ DOC", "Revenue", "Status", "Calculation"]
                table_rows = [[
                    str(r.get("date", "")),
                    r.get("asin", ""),
                    r.get("category", ""),
                    r.get("portfolio", ""),
                    r.get("amz_fba", 0),
                    r.get("amz_flex", 0),
                    r.get("total_amz_stock", 0),
                    r.get("amz_sales", 0),
                    r.get("amz_doc", 0),
                    r.get("amz_revenue", 0),
                    r.get("amz_status", ""),
                    r.get("reason", ""),
                ] for r in rows]
            elif _platform == "Flipkart":
                headers = ["Date", "FSN", "Category", "Portfolio", "FK FBF", "FK Flex", "Total FK Stock", "FK Sales", "FK DOC", "Revenue", "Status", "Calculation"]
                table_rows = [[
                    str(r.get("date", "")),
                    r.get("fsn", ""),
                    r.get("category", ""),
                    r.get("portfolio", ""),
                    r.get("fk_fbf", 0),
                    r.get("fk_flex", 0),
                    r.get("total_fk_stock", 0),
                    r.get("fk_sales", 0),
                    r.get("fk_doc", 0),
                    r.get("fk_revenue", 0),
                    r.get("fk_status", ""),
                    r.get("fk_reason", ""),
                ] for r in rows]
            else:  # All platforms
                headers = ["Date", "ASIN", "FSN", "Category", "Portfolio", "AMZ FBA", "AMZ Flex", "Total AMZ Stock", "FK FBF", "FK Flex", "Total FK Stock", "Total Stock (AMZ+FK)", "Total Sales", "DOC", "AMZ Revenue", "FK Revenue", "AMZ Status", "FK Status", "Calculation"]
                table_rows = [[
                    str(r.get("date", "")),
                    r.get("asin", ""),
                    r.get("fsn", ""),
                    r.get("category", ""),
                    r.get("portfolio", ""),
                    r.get("amz_fba", 0),
                    r.get("amz_flex", 0),
                    r.get("total_amz_stock", 0),
                    r.get("fk_fbf", 0),
                    r.get("fk_flex", 0),
                    r.get("total_fk_stock", 0),
                    r.get("total_stock", 0),
                    r.get("total_sales", 0),
                    r.get("total_doc", 0),
                    r.get("amz_revenue", 0),
                    r.get("fk_revenue", 0),
                    r.get("amz_status", ""),
                    r.get("fk_status", ""),
                    r.get("total_reason", ""),
                ] for r in rows]
        elif modal_key == "npd-performance":
            headers, table_rows = _npd_export_table(rows, filters.get("platform") or "All")
        elif modal_key == "top-products":
            _platform = filters.get("platform") or "All"
            if _platform == "Amazon":
                headers = ["MSKU", "ASIN", "Portfolio", "Revenue", "Prev Revenue", "MoM Growth %", "Contribution %", "Page Views", "Units Sold"]
                table_rows = [[
                    r.get("msku", ""), r.get("az_sku", ""), r.get("cluster", ""),
                    r.get("az_revenue", 0), r.get("az_prev_revenue", 0),
                    r.get("az_mom_growth", 0), r.get("az_contribution", 0),
                    r.get("az_pageviews", 0), r.get("units_sold", 0)
                ] for r in rows]
            elif _platform == "Flipkart":
                headers = ["MSKU", "FSN", "Portfolio", "Revenue", "Prev Revenue", "MoM Growth %", "Contribution %", "Page Views", "Units Sold"]
                table_rows = [[
                    r.get("msku", ""), r.get("fk_sku", ""), r.get("cluster", ""),
                    r.get("fk_revenue", 0), r.get("fk_prev_revenue", 0),
                    r.get("fk_mom_growth", 0), r.get("fk_contribution", 0),
                    r.get("fk_pageviews", 0), r.get("fk_units", 0)
                ] for r in rows]
            else:
                headers = ["SKU", "ASIN", "FSN", "AMZ Revenue", "FK Revenue", "Total", "AMZ MOM Growth", "FK MOM Growth", "AMZ Contribution", "FK Contribution", "AMZ Page Views", "FK Page Views"]
                table_rows = [[
                    r.get("msku", ""), r.get("az_sku", ""), r.get("fk_sku", ""),
                    r.get("az_revenue", 0), r.get("fk_revenue", 0), r.get("revenue", 0),
                    r.get("az_mom_growth", 0), r.get("fk_mom_growth", 0),
                    r.get("az_contribution", 0), r.get("fk_contribution", 0),
                    r.get("az_pageviews", 0), r.get("fk_pageviews", 0)
                ] for r in rows]
        elif modal_key == "declining-products":
            _platform = filters.get("platform") or "All"
            if _platform == "Amazon":
                headers = ["MSKU", "ASIN", "MOM Drop %", "Impact", "Page Views"]
                table_rows = [[
                    r.get("msku", ""), r.get("az_sku", ""),
                    r.get("az_drop_pct", 0), r.get("az_impact", 0), r.get("az_pageviews", 0)
                ] for r in rows]
            elif _platform == "Flipkart":
                headers = ["MSKU", "FSN", "MOM Drop %", "Impact", "Page Views"]
                table_rows = [[
                    r.get("msku", ""), r.get("fk_sku", ""),
                    r.get("fk_drop_pct", 0), r.get("fk_impact", 0), r.get("fk_pageviews", 0)
                ] for r in rows]
            else:
                headers = ["SKU", "ASIN", "FSN", "AMZ MOM Drop %", "FK MOM Drop %", "Total MOM Drop %", "AMZ Impact", "FK Impact", "Total Impact", "AMZ Page Views", "FK Page Views", "Total Page Views"]
                table_rows = [[
                    r.get("msku", ""), r.get("az_sku", ""), r.get("fk_sku", ""),
                    r.get("az_drop_pct", 0), r.get("fk_drop_pct", 0), r.get("drop_pct", 0),
                    r.get("az_impact", 0), r.get("fk_impact", 0), r.get("impact", 0),
                    r.get("az_pageviews", 0), r.get("fk_pageviews", 0), r.get("pageviews", 0)
                ] for r in rows]
        else:
            headers, table_rows = _rows_to_export_table(rows)

        if export_format == "csv":
            output = StringIO()
            writer = csv.writer(output)
            writer.writerow(headers)
            writer.writerows(table_rows)
            buf = BytesIO(output.getvalue().encode("utf-8"))
            response = FileResponse(buf, content_type="text/csv")
            response["Content-Disposition"] = (
                f'attachment; filename="{_modal_rows_export_filename(view_name, modal_key, "csv")}"'
            )
            return response

        buf = BytesIO()
        pd.DataFrame(table_rows, columns=headers).to_excel(buf, index=False)
        buf.seek(0)
        response = FileResponse(
            buf,
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        response["Content-Disposition"] = (
            f'attachment; filename="{_modal_rows_export_filename(view_name, modal_key, "xlsx")}"'
        )
        return response

    # ── DataTables server-side path ──────────────────────────────────────────
    if dt_draw:
        order_field = (request.GET.get("order_field") or "").strip()
        order_dir = (request.GET.get("order_dir") or "desc").strip().lower()

        if modal_key == "inventory-health":
            # inventory-health uses ORM — apply ordering and re-slice
            if order_field:
                _orm_field = order_field
                if order_dir == "desc":
                    _orm_field = f"-{order_field}"
                inventory_qs = _get_inventory_modal_queryset(data_owner, filters, query)
                total = inventory_qs.count()
                row_qs = inventory_qs.order_by(_orm_field)[(page - 1) * page_size : page * page_size]
                from apps.dashboard.models import CategoryMapping, FlipkartCategoryMap
                _inv_msku_map = {}
                for _r in CategoryMapping.objects.filter(user=data_owner).values("asin", "msku"):
                    if _r["asin"] and _r["msku"]:
                        _inv_msku_map[_r["asin"]] = _r["msku"]
                for _r in FlipkartCategoryMap.objects.filter(user=data_owner).values("fsn", "sku"):
                    if _r["fsn"] and _r["sku"]:
                        _inv_msku_map[_r["fsn"]] = _r["sku"]
                rows = [_inventory_summary_row_dict(row, msku_map=_inv_msku_map) for row in row_qs]
            rows_total = total
            page_rows = rows
        else:
            # List-based modals: sort in Python then paginate
            if order_field:
                rows = _sort_rows_by_field(rows or [], order_field, order_dir)
            rows_total, page_rows = _paginate_rows(rows or [], page, page_size)

        html = render_to_string(
            template_name,
            {
                "rows": page_rows,
                "rows_total": rows_total,
                "rows_shown": len(page_rows),
                "rows_truncated": False,
                "platform": (filters.get("platform") or "All"),
            },
            request=request,
        )
        dt_data = _extract_dt_cells_from_html(html)
        return JsonResponse({
            "draw": int(dt_draw),
            "recordsTotal": rows_total,
            "recordsFiltered": rows_total,
            "data": dt_data,
        })

    # ── Legacy HTML/pagination path ───────────────────────────────────────────
    if modal_key == "inventory-health":
        rows_total, page_rows = total, rows
        total_pages = math.ceil(rows_total / page_size) if rows_total > 0 else 0
    elif load_all:
        # DataTables client-side: send all rows at once, no server pagination.
        page_rows = rows
        rows_total = total
        total_pages = 1
    else:
        rows_total, page_rows = _paginate_rows(rows, page, page_size)
        total_pages = math.ceil(rows_total / page_size) if rows_total > 0 else 0

    html = render_to_string(
        template_name,
        {
            "rows": page_rows,
            "rows_total": rows_total,
            "rows_shown": len(page_rows),
            "rows_truncated": len(page_rows) < rows_total,
            "platform": (filters.get("platform") or "All"),
        },
        request=request,
    )
    payload = {
        "html": html,
        "use_datatable": load_all,
        "pagination": {
            "page": page,
            "page_size": page_size,
            "total": rows_total,
            "total_pages": total_pages,
            "has_prev": page > 1,
            "has_next": page < total_pages,
        },
    }
    if not load_all:
        cache.set(modal_rows_cache_key, payload, timeout=modal_rows_cache_ttl)
    return JsonResponse(payload)



@no_cache_for_htmx
def dashboard_product_card_rows_view(request, view_name, card_key):
    user = get_logged_in_user(request)
    if not user:
        return JsonResponse({"error": "Not authenticated"}, status=401)

    feature_code = DASHBOARD_FEATURE_BY_VIEW.get(view_name)
    if not feature_code:
        return JsonResponse({"error": "Invalid dashboard view."}, status=404)
    if not _user_has_feature(user, feature_code):
        return JsonResponse({"error": "Permission denied."}, status=403)

    template_name = DASHBOARD_PRODUCT_CARD_TEMPLATE_MAP.get((view_name, card_key))
    if not template_name:
        return JsonResponse({"error": "Invalid product card."}, status=404)
    payload_key = DASHBOARD_PRODUCT_CARD_PAYLOAD_KEY_MAP.get(card_key)
    if not payload_key:
        return JsonResponse({"error": "Invalid product card."}, status=404)

    data_owner = user.created_by if user.created_by else user
    data_version = cache.get(f"dashboard_data_version_{data_owner.id}", 0)
    filters = _strip_non_dashboard_filters(build_filters_from_querydict(request.GET))
    filter_hash = hashlib.md5(cache_filter_string(filters).encode("utf-8")).hexdigest()
    cache_key = (
        "dashboard_product_card_rows_v3_"
        f"{data_owner.id}_{view_name}_{card_key}_{data_version}_{filter_hash}"
    )

    cached_html = cache.get(cache_key)
    if cached_html:
        return HttpResponse(cached_html)

    if card_key == "top-products":
        rows = _get_top_product_modal_rows(data_owner, filters)
    elif card_key == "declining-products":
        rows = _get_declining_product_modal_rows(data_owner, filters)
    elif card_key == "npd-performance":
        rows = _get_npd_modal_rows(data_owner, filters)
    else:
        rows = []

    if not isinstance(rows, list):
        rows = []
        
    limit_str = request.GET.get('limit')
    limit = 10
    if limit_str and limit_str.isdigit():
        limit = int(limit_str)
    
    rows = rows[:limit]

    platform = filters.get("platform") or "All"
    html = render_to_string(template_name, {"rows": rows, "platform": platform}, request=request)
    cache.set(cache_key, html, timeout=300)
    return HttpResponse(html)


@no_cache_for_htmx
def dashboard_category_performance_rows_view(request, view_name):
    user = get_logged_in_user(request)
    if not user:
        return JsonResponse({"error": "Not authenticated"}, status=401)

    feature_code = DASHBOARD_FEATURE_BY_VIEW.get(view_name)
    if not feature_code:
        return JsonResponse({"error": "Invalid dashboard view."}, status=404)
    if not _user_has_feature(user, feature_code):
        return JsonResponse({"error": "Permission denied."}, status=403)

    template_name = DASHBOARD_CATEGORY_PERFORMANCE_ROWS_TEMPLATE_MAP.get(view_name)
    if not template_name:
        return JsonResponse({"error": "Invalid dashboard view."}, status=404)

    page = _parse_positive_int(request.GET.get("page"), default=1, minimum=1, maximum=10_000)
    page_size = _parse_positive_int(
        request.GET.get("page_size"), default=10, minimum=1, maximum=50
    )
    query = (request.GET.get("q") or "").strip().lower()
    export_format = (request.GET.get("export") or "").strip().lower()

    ctx = get_dashboard_context(
        request,
        include_payload=True,
        cache_view_type=f"{view_name}-dashboard",
        section_scope="details",
        compute_scope="analytics",
    )
    if ctx is None:
        return JsonResponse({"error": "Not authenticated"}, status=401)

    rows = (ctx.get("payload") or {}).get("category_performance") or []
    if query:
        rows = [r for r in rows if query in str(r.get("category", "")).lower()]

    if export_format in {"csv", "excel", "xlsx"}:
        headers, table_rows = _category_performance_export_table(rows)
        if export_format == "csv":
            output = StringIO()
            writer = csv.writer(output)
            writer.writerow(headers)
            writer.writerows(table_rows)
            buf = BytesIO(output.getvalue().encode("utf-8"))
            response = FileResponse(buf, content_type="text/csv")
            response["Content-Disposition"] = (
                f'attachment; filename="{_category_performance_export_filename(view_name, "csv")}"'
            )
            return response

        buf = BytesIO()
        pd.DataFrame(table_rows, columns=headers).to_excel(buf, index=False)
        buf.seek(0)
        response = FileResponse(
            buf,
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        response["Content-Disposition"] = (
            f'attachment; filename="{_category_performance_export_filename(view_name, "xlsx")}"'
        )
        return response

    rows = rows[:10]
    total = len(rows)
    start = (page - 1) * page_size
    page_rows = rows[start : start + page_size]
    total_pages = math.ceil(total / page_size) if total > 0 else 0

    html = render_to_string(
        template_name,
        {
            "rows": page_rows,
            "start_index": start,
        },
        request=request,
    )
    return JsonResponse(
        {
            "html": html,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total": total,
                "total_pages": total_pages,
                "has_prev": page > 1,
                "has_next": page < total_pages,
            },
        }
    )


@no_cache_for_htmx
def dashboard_asin_fsn_report_rows_view(request):
    user = get_logged_in_user(request)
    if not user:
        return JsonResponse({"error": "Not authenticated"}, status=401)
    if not _user_has_feature(user, "category_dashboard"):
        return JsonResponse({"error": "Permission denied."}, status=403)

    data_owner = user.created_by if user.created_by else user
    page = _parse_positive_int(request.GET.get("page"), default=1, minimum=1, maximum=10_000)
    page_size = _parse_positive_int(
        request.GET.get("page_size"), default=10, minimum=1, maximum=100
    )
    report_limit = _parse_positive_int(
        request.GET.get("report_limit"), default=10, minimum=1, maximum=100
    )
    report_date_range = (request.GET.get("report_date_range") or "all").strip().lower()
    report_start_date = request.GET.get("report_start_date") or ""
    report_end_date = request.GET.get("report_end_date") or ""
    export_format = (request.GET.get("export") or "").strip().lower()
    load_all = request.GET.get("all") == "1"
    filters = _strip_non_dashboard_filters(build_filters_from_querydict(request.GET))

    data_version = cache.get(f"dashboard_data_version_{data_owner.id}", 0)
    filter_hash = hashlib.md5(cache_filter_string(filters).encode("utf-8")).hexdigest()
    report_cache_key = (
        "dashboard_asin_fsn_report_rows_v4_"
        f"{data_owner.id}_{data_version}_{filter_hash}_{report_date_range}_{report_start_date}_{report_end_date}_{report_limit}"
    )
    report_cache_ttl = int(
        getattr(settings, "DASHBOARD_MODAL_ROWS_CACHE_TTL_SECONDS", 180)
    )
    cached_rows = cache.get(report_cache_key)
    if cached_rows is None:
        cached_rows = _get_asin_fsn_report_rows(
            data_owner=data_owner,
            filters=filters,
            report_date_range=report_date_range,
            report_limit=report_limit,
            report_start_date=report_start_date,
            report_end_date=report_end_date,
        )
        cache.set(report_cache_key, cached_rows or [], timeout=report_cache_ttl)
    rows = cached_rows or []

    if export_format in {"csv", "excel", "xlsx"}:
        headers, table_rows = _asin_fsn_report_export_table(rows)
        if export_format == "csv":
            output = StringIO()
            writer = csv.writer(output)
            writer.writerow(headers)
            writer.writerows(table_rows)
            buf = BytesIO(output.getvalue().encode("utf-8"))
            response = FileResponse(buf, content_type="text/csv")
            response["Content-Disposition"] = (
                f'attachment; filename="{_asin_fsn_report_export_filename("csv")}"'
            )
            return response

        buf = BytesIO()
        pd.DataFrame(table_rows, columns=headers).to_excel(buf, index=False)
        buf.seek(0)
        response = FileResponse(
            buf,
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        response["Content-Disposition"] = (
            f'attachment; filename="{_asin_fsn_report_export_filename("xlsx")}"'
        )
        return response

    total = len(rows)
    if load_all:
        page_rows = rows
        total_pages = 1 if total else 0
    else:
        start = (page - 1) * page_size
        page_rows = rows[start : start + page_size]
        total_pages = math.ceil(total / page_size) if total > 0 else 0
    html = render_to_string(
        "dashboard/partials/asin_fsn_report_rows.html",
        {"rows": page_rows},
        request=request,
    )
    return JsonResponse(
        {
            "html": html,
            "use_datatable": load_all,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total": total,
                "total_pages": total_pages,
                "has_prev": (not load_all) and page > 1,
                "has_next": (not load_all) and page < total_pages,
            },
        }
    )


@require_feature("upload_data")
def upload_view(request):
    user = get_logged_in_user(request)
    if not user:
        return redirect("account-login")
    data_owner = user.created_by if user.created_by else user

    if user.is_main_user:
        _feat_key = "all_feature_codenames_v1"
        user_features = cache.get(_feat_key)
        if user_features is None:
            user_features = list(Feature.objects.values_list("code_name", flat=True))
            cache.set(_feat_key, user_features, timeout=3600)
    else:
        if user.role:
            _feat_key = f"role_feature_codenames_v1_{user.role_id}"
            user_features = cache.get(_feat_key)
            if user_features is None:
                user_features = list(
                    user.role.features.values_list("code_name", flat=True)
                )
                cache.set(_feat_key, user_features, timeout=3600)
        else:
            user_features = []
    from apps.upload.models import UploadLog
    upload_logs = UploadLog.objects.filter(data_owner=data_owner).select_related(
        "uploaded_by"
    )[:100]

    upload_task_timeout_seconds = int(
        getattr(settings, "UPLOAD_TASK_TIMEOUT_SECONDS", 1800)
    )

    return render(
        request,
        "dashboard/upload.html",
        {
            "logged_user": user,
            "user_features": user_features,
            "upload_logs": upload_logs,
            "payload_json": "null",
            "selected_filters_json": "{}",
            "dashboard_refresh_status_json": '{"state":"idle","message":""}',
            "upload_task_timeout_ms": max(upload_task_timeout_seconds, 60) * 1000,
        },
    )


def dashboard_refresh_status(request):
    user = get_logged_in_user(request)
    if not user:
        return JsonResponse({"error": "Not authenticated"}, status=401)
    data_owner = user.created_by if user.created_by else user
    return JsonResponse(_get_dashboard_refresh_status(data_owner.id))


@require_GET
def dashboard_refresh_now(request):
    user = get_logged_in_user(request)
    if not user:
        return JsonResponse({"error": "Not authenticated"}, status=401)

    data_owner = user.created_by if user.created_by else user
    invalidate_dashboard_cache_for_user(data_owner.id, clear_materialized=True)

    response = JsonResponse(
        {
            "ok": True,
            "message": "Dashboard cache cleared. Reloading fresh data.",
        }
    )
    response["Cache-Control"] = "no-cache, no-store, must-revalidate, max-age=0"
    response["Pragma"] = "no-cache"
    response["Expires"] = "0"
    return response


def _parse_positive_int(value, default, minimum=1, maximum=200):
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    if parsed < minimum:
        return minimum
    if parsed > maximum:
        return maximum
    return parsed


def _distinct_option_values_qs(qs, field_name):
    return (
        qs.annotate(option_value=F(field_name))
        .values("option_value")
        .distinct()
    )


def _search_paginated_single_source(base_qs, field_name, q, offset, page_size):
    values_qs = _distinct_option_values_qs(base_qs, field_name)
    if not q:
        ordered = values_qs.order_by("option_value")
    else:
        ordered = values_qs.filter(
            option_value__icontains=q
        ).annotate(
            starts_with_q=Case(
                When(option_value__istartswith=q, then=Value(0)),
                default=Value(1),
                output_field=IntegerField()
            )
        ).order_by("starts_with_q", "option_value")

    rows = list(ordered[offset : offset + page_size + 1])
    has_next = len(rows) > page_size
    rows = rows[:page_size]
    total = offset + page_size + (1 if has_next else 0)
    return total, [str(r["option_value"]).strip() for r in rows if r["option_value"]]


def _search_paginated_dual_source(az_qs, fk_qs, field_name, q, offset, page_size, az_field=None, fk_field=None):
    az_field = az_field or field_name
    fk_field = fk_field or field_name
    az_values = _distinct_option_values_qs(az_qs, az_field)
    fk_values = _distinct_option_values_qs(fk_qs, fk_field)
    
    if not q:
        merged_qs = az_values.union(fk_values).order_by("option_value")
    else:
        az_q = az_values.filter(option_value__icontains=q).annotate(
            starts_with_q=Case(
                When(option_value__istartswith=q, then=Value(0)),
                default=Value(1),
                output_field=IntegerField()
            )
        )
        fk_q = fk_values.filter(option_value__icontains=q).annotate(
            starts_with_q=Case(
                When(option_value__istartswith=q, then=Value(0)),
                default=Value(1),
                output_field=IntegerField()
            )
        )
        merged_qs = az_q.union(fk_q).order_by("starts_with_q", "option_value")

    rows = list(merged_qs[offset : offset + page_size + 1])
    has_next = len(rows) > page_size
    rows = rows[:page_size]
    total = offset + page_size + (1 if has_next else 0)
    return total, [str(r["option_value"]).strip() for r in rows if r["option_value"]]


def filter_dropdown_options(request):
    """
    Paginated + search-backed filter option endpoint.
    Uses the currently applied dashboard filters (except the requested field)
    so dropdown options remain context-aware.
    """
    user = get_logged_in_user(request)
    if not user:
        return JsonResponse({"error": "Not authenticated"}, status=401)
    if not user.is_main_user:
        if not user.role:
            return JsonResponse({"error": "Permission denied."}, status=403)
        has_dashboard_feature = user.role.features.filter(
            code_name__in={"business_dashboard", "ceo_dashboard", "category_dashboard"}
        ).exists()
        if not has_dashboard_feature:
            return JsonResponse({"error": "Permission denied."}, status=403)

    data_owner = user.created_by if user.created_by else user
    field = (request.GET.get("field") or "").strip().lower()
    if field not in LIST_FILTER_FIELDS:
        return JsonResponse({"error": "Invalid field."}, status=400)

    q = (request.GET.get("q") or "").strip()
    page = _parse_positive_int(request.GET.get("page"), default=1, minimum=1, maximum=10_000)
    page_size = _parse_positive_int(
        request.GET.get("page_size"), default=50, minimum=10, maximum=100
    )
    offset = (page - 1) * page_size

    filters = build_filters_from_querydict(request.GET)
    filters.pop("field", None)
    filters.pop("q", None)
    filters.pop("page", None)
    filters.pop("page_size", None)
    # Don't self-filter the requested dropdown field.
    filters.pop(field, None)

    data_version = cache.get(f"dashboard_data_version_{data_owner.id}", 0)
    dropdown_cache_hash = hashlib.md5(
        cache_filter_string(filters).encode("utf-8")
    ).hexdigest()
    dropdown_cache_key = (
        f"dashboard_filter_options_v3_{data_owner.id}_{data_version}_{field}_"
        f"{hashlib.md5(q.lower().encode('utf-8')).hexdigest()}_{page}_{page_size}_{dropdown_cache_hash}"
    )
    cached_payload = cache.get(dropdown_cache_key)
    if cached_payload:
        return JsonResponse(cached_payload)

    from apps.dashboard.services.filters import get_filtered_mapping_querysets
    az_map_qs, fk_map_qs = get_filtered_mapping_querysets(filters, user=data_owner)

    results = []
    total = 0

    if field == "asin":
        asin_qs = az_map_qs.exclude(asin__isnull=True).exclude(asin="")
        total, results = _search_paginated_single_source(
            asin_qs, "asin", q, offset, page_size
        )
    elif field == "fsn":
        fsn_qs = fk_map_qs.exclude(fsn__isnull=True).exclude(fsn="")
        total, results = _search_paginated_single_source(
            fsn_qs, "fsn", q, offset, page_size
        )
    elif field == "parent_asin":
        az_parent_qs = az_map_qs.exclude(parent_asin__isnull=True).exclude(parent_asin="")
        linked_fk_asins = fk_map_qs.exclude(asin__isnull=True).exclude(asin="").values("asin")
        fk_parent_qs = CategoryMapping.objects.filter(
            user=data_owner,
            asin__in=linked_fk_asins,
        ).exclude(parent_asin__isnull=True).exclude(parent_asin="")
        total, results = _search_paginated_dual_source(
            az_parent_qs,
            fk_parent_qs,
            "parent_asin",
            q,
            offset,
            page_size,
        )
    elif field == "sku":
        az_sku_qs = az_map_qs.exclude(msku__isnull=True).exclude(msku="")
        fk_sku_qs = fk_map_qs.exclude(sku__isnull=True).exclude(sku="")
        total, results = _search_paginated_dual_source(
            az_sku_qs,
            fk_sku_qs,
            "sku",
            q,
            offset,
            page_size,
            az_field="msku",
            fk_field="sku",
        )
    elif field == "ratings":
        ranges = ["0 - 1", "1 - 3", "3 - 3.5", "3.5 - 4", "4 - 4.2", "4.2 - 4.5", "4.5+"]
        if q:
            ranges = [r for r in ranges if q.lower() in r.lower()]
        total = len(ranges)
        results = ranges[offset : offset + page_size]
    elif field in {
        "category_manager",
        "series_name",
        "material",
        "size",
        "brand_name",
        "finish",
        "category",
        "portfolio",
        "subcategory",
    }:
        az_field_qs = az_map_qs.exclude(**{f"{field}__isnull": True}).exclude(**{field: ""}).exclude(**{field: "0"})
        fk_field_qs = fk_map_qs.exclude(**{f"{field}__isnull": True}).exclude(**{field: ""}).exclude(**{field: "0"})
        total, results = _search_paginated_dual_source(
            az_field_qs,
            fk_field_qs,
            field,
            q,
            offset,
            page_size,
        )
    elif field == "inventory_health":
        from apps.dashboard.models import DashboardInventoryHealthSummary
        from django.db.models import Max
        max_date = DashboardInventoryHealthSummary.objects.filter(user=data_owner).aggregate(Max('date'))['date__max']
        az_statuses = []
        fk_statuses = []
        if max_date:
            az_statuses = DashboardInventoryHealthSummary.objects.filter(
                user=data_owner, date=max_date, asin__in=az_map_qs.values("asin")
            ).exclude(status="").values_list("status", flat=True).distinct()
            fk_statuses = DashboardInventoryHealthSummary.objects.filter(
                user=data_owner, date=max_date, fsn__in=fk_map_qs.values("fsn")
            ).exclude(fk_status="").values_list("fk_status", flat=True).distinct()
        all_statuses = sorted(list(set(az_statuses) | set(fk_statuses)))
        if q:
            all_statuses = [s for s in all_statuses if q.lower() in s.lower()]
        total = len(all_statuses)
        results = all_statuses[offset : offset + page_size]
    else:
        total, results = 0, []
    payload = {
        "field": field,
        "results": [{"value": value, "label": value} for value in results],
        "pagination": {
            "page": page,
            "page_size": page_size,
            "total": total,
            "has_next": offset + page_size < total,
        },
    }
    cache.set(dropdown_cache_key, payload, timeout=300)
    return JsonResponse(payload)


def _demo_specs(today):
    day_ddmmyyyy = today.strftime("%d-%m-%Y")
    day_ymd = today.strftime("%Y-%m-%d")
    return {
        # Upload Data (Amazon)
        "upload_sales": {
            "kind": "csv",
            "filename": f"{day_ddmmyyyy}.csv",
            "columns": [
                "(Child) ASIN",
                "Page Views - Total",
                "Units Ordered",
                "Ordered Product Sales",
                "Total Order Items",
            ],
            "rows": [["B0DEMOASIN1", 245, 18, "₹25,499.00", 17]],
        },
        "upload_category": {
            "kind": "csv",
            "filename": "category_mapping_demo.csv",
            "columns": ["ASIN", "Portfolio", "Category", "Subcategory", "Skus"],
            "rows": [["B0DEMOASIN1", "Home", "Storage", "Bins", "SKU-DEMO-1"]],
        },
        "upload_spend": {
            "kind": "csv",
            "filename": "ads_spend_demo.csv",
            "columns": ["Date", "Ad Account", "Ad Type", "ASIN", "Spend"],
            "rows": [[day_ymd, "Main Ads", "SP", "B0DEMOASIN1", 1250.50]],
        },
        "upload_price": {
            "kind": "csv",
            "filename": "pricing_data_demo.csv",
            "columns": ["ASIN", "Price"],
            "rows": [["B0DEMOASIN1", 1499]],
        },
        "upload_fba_stock": {
            "kind": "csv",
            "filename": "fba_stock_demo.csv",
            "columns": [
                "Date",
                "FNSKU",
                "ASIN",
                "MSKU",
                "Title",
                "Disposition",
                "Starting Warehouse Balance",
                "In Transit Between Warehouses",
                "Receipts",
                "Customer Shipments",
                "Customer Returns",
                "Vendor Returns",
                "Warehouse Transfer In/Out",
                "Found",
                "Lost",
                "Damaged",
                "Disposed",
                "Other Events",
                "Ending Warehouse Balance",
                "Unknown Events",
                "Location",
            ],
            "rows": [
                [
                    day_ymd,
                    "X000DEMOFNSKU",
                    "B0DEMOASIN1",
                    "MSKU-DEMO-1",
                    "Demo Product",
                    "SELLABLE",
                    120,
                    10,
                    15,
                    8,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    137,
                    0,
                    "DEL4",
                ]
            ],
        },
        "upload_flex_stock": {
            "kind": "csv",
            "filename": "flex_stock_demo.csv",
            "columns": ["Date", "ASIN", "Cluster", "Qty"],
            "rows": [[day_ymd, "B0DEMOASIN1", "BANGALORE", 42]],
        },
        # Upload Data (Flipkart)
        "fk_search_traffic": {
            "kind": "csv",
            "filename": "fk_search_traffic_demo.csv",
            "columns": [
                "Listing Id",
                "SKU Id",
                "Vertical",
                "Impression Date",
                "Product Clicks",
                "Sales",
                "Revenue",
            ],
            "rows": [["ABCDEMOFSN00000001X", "FK-SKU-1", "Home Furnishing", day_ymd, 320, 22, 28765]],
        },
        "fk_category": {
            "kind": "csv",
            "filename": "fk_category_demo.csv",
            "columns": [
                "FSN ID",
                "SKU",
                "Portfolio",
                "Category",
                "Sub Category",
                "Vertical",
                "Product Status",
            ],
            "rows": [
                [
                    "DEMOFSN00000001",
                    "FK-SKU-1",
                    "Home",
                    "Storage",
                    "Bins",
                    "Home Furnishing",
                    "Continue",
                ]
            ],
        },
        "fk_price": {
            "kind": "csv",
            "filename": "fk_price_demo.csv",
            "columns": ["Flipkart Serial Number", "Deal"],
            "rows": [["DEMOFSN00000001", 1349]],
        },
        "fk_pla": {
            "kind": "csv_with_metadata",
            "filename": "fk_pla_demo.csv",
            "metadata_rows": [["Start Time,2026-01-01 00:00:00"], ["End Time,2026-01-01 23:59:59"]],
            "columns": ["Campaign ID", "Advertised FSN ID", "Ad Spend"],
            "rows": [["CMP-1001", "DEMOFSN00000001", 842.75]],
        },
        "fk_fba_stock": {
            "kind": "csv",
            "filename": "fk_fba_stock_demo.csv",
            "columns": [
                "Date",
                "Warehouse Id",
                "SKU",
                "Title",
                "Listing Id",
                "FSN",
                "Brand",
                "Flipkart Selling Price",
                "Live on Website",
            ],
            "rows": [
                [
                    day_ymd,
                    "blr_main_wh",
                    "FK-SKU-1",
                    "Demo FK Product",
                    "LSTDEMOFSN00000001XYZ",
                    "DEMOFSN00000001",
                    "Plantex",
                    1349,
                    87,
                ]
            ],
        },
        "fk_inventory": {
            "kind": "xlsx",
            "filename": "fk_inventory_demo.xlsx",
            "columns": ["PRODUCTS STATUS", "PRODUCTS TYPE", "SKU", "FSN", "Qty"],
            "rows": [["Continued", "Storage", "FK-SKU-1", "DEMOFSN00000001", 42]],
        },
        # Replenishment
        "repl_sales": {
            "kind": "csv",
            "filename": "replenishment_sales_demo.csv",
            "columns": [
                "FC CODE",
                "Shipment To Postal Code",
                "ASIN",
                "Customer Shipment Date",
                "Quantity",
                "Amazon Order ID",
                "Product Amount",
                "Shipping Amount",
                "Gift Amount",
            ],
            "rows": [["DEL4", "560001", "B0DEMOASIN1", f"{day_ymd}T10:10:00+05:30", 2, "AMZ-ORD-1001", 1499, 40, 0]],
        },
        "repl_stock": {
            "kind": "xlsx",
            "filename": "replenishment_stock_demo.xlsx",
            "columns": [
                "ASIN",
                "Location",
                "Disposition",
                "Ending Warehouse Balance",
                "In Transit Between Warehouses",
            ],
            "rows": [["B0DEMOASIN1", "DEL4", "sellable", 150, 12]],
        },
        "repl_lis": {
            "kind": "xlsx",
            "filename": "replenishment_lis_demo.xlsx",
            "columns": ["ASIN", "Cluster", "Sum of Local Shipped Units", "Sum of Total Units"],
            "rows": [["B0DEMOASIN1", "BANGALORE", 18, 30]],
        },
        "repl_shipment": {
            "kind": "xlsx",
            "filename": "replenishment_shipment_demo.xlsx",
            "columns": [
                "ASIN",
                "CLUSTER",
                "FC",
                "STATUS",
                "FINAL QTY",
                "ID",
                "APPOINTMENT DATE",
                "LOADING DATE",
            ],
            "rows": [["B0DEMOASIN1", "BANGALORE", "DEL4", "Upcoming", 45, "SHP-1001", day_ymd, day_ymd]],
        },
        "repl_assortment": {
            "kind": "xlsx",
            "filename": "replenishment_assortment_demo.xlsx",
            "columns": [
                "ASIN",
                "SKU",
                "HSN CODE",
                "VENDOR NAME",
                "PRODUCTS STATUS",
                "ACT WEIGHT",
                "VOLUMETRIC WEIGHT",
                "PRODUCT TYPE",
                "PRODUCT SIZE",
                "Portfolio",
                "Category",
                "Brand",
            ],
            "rows": [["B0DEMOASIN1", "SKU-DEMO-1", "392490", "Demo Vendor", "Active", 0.75, 1.10, "Storage", "Medium", "Home", "Bins", "Plantex"]],
        },
        "repl_fc_cluster": {
            "kind": "xlsx",
            "filename": "replenishment_fc_cluster_demo.xlsx",
            "columns": ["FC CODE", "FC TYPE", "CLUSTER NAME", "ZONE"],
            "rows": [["DEL4", "AMAZON", "DELHI", "North"]],
        },
        "repl_pincode_cluster": {
            "kind": "csv",
            "filename": "replenishment_pincode_cluster_demo.csv",
            "columns": ["PIN CODE", "Fulfilment Cluster", "IDEAL CLUSTER", "ZONE"],
            "rows": [["560001", "BANGALORE", "BLR_CLUSTER", "South"]],
        },
        "repl_input_sheet": {
            "kind": "xlsx",
            "filename": "replenishment_input_sheet_demo.xlsx",
            "columns": ["Particular", "Value"],
            "rows": [
                ["P0 Demand DOC", "15 Days"],
                ["P1 Demand DOC", "30 Days"],
                ["P2 Demand DOC", "60 Days"],
                ["Sale Report Days", "7 Days"],
                ["Stock Report Date", day_ymd],
            ],
        },
        "repl_business_report": {
            "kind": "csv",
            "filename": "replenishment_business_report_demo.csv",
            "columns": [
                "(Child) ASIN",
                "Page Views - Total",
                "Units Ordered",
                "Ordered Product Sales",
                "Total Order Items",
            ],
            "rows": [["B0DEMOASIN1", 180, 14, "₹19,999.00", 13]],
        },
        "repl_flex_qty": {
            "kind": "csv",
            "filename": "replenishment_flex_qty_demo.csv",
            "columns": ["ASIN", "Cluster", "Qty"],
            "rows": [["B0DEMOASIN1", "BANGALORE", 20]],
        },
    }


def _table_to_dataframe(columns, rows):
    return pd.DataFrame(rows, columns=columns)


def download_demo_template(request):
    user = get_logged_in_user(request)
    if not user:
        return redirect("account-login")

    template_key = (request.GET.get("template") or "").strip()
    specs = _demo_specs(datetime.date.today())
    spec = specs.get(template_key)
    if not spec:
        return JsonResponse(
            {"error": "Invalid template key. Please provide a valid template."},
            status=400,
        )

    kind = spec["kind"]
    filename = spec["filename"]

    if kind in {"csv", "csv_with_metadata"}:
        output = StringIO()
        if kind == "csv_with_metadata":
            for row in spec.get("metadata_rows", []):
                output.write(",".join(str(v) for v in row) + "\n")
        writer = csv.writer(output)
        writer.writerow(spec["columns"])
        writer.writerows(spec["rows"])
        data = output.getvalue().encode("utf-8")
        buf = BytesIO(data)
        response = FileResponse(buf, content_type="text/csv")
    elif kind == "xlsx":
        buf = BytesIO()
        df = _table_to_dataframe(spec["columns"], spec["rows"])
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Sheet1")
        buf.seek(0)
        response = FileResponse(
            buf,
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    elif kind == "xlsx_multi":
        buf = BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            for sheet_name, sheet in spec["sheets"].items():
                df = _table_to_dataframe(sheet["columns"], sheet["rows"])
                df.to_excel(writer, index=False, sheet_name=sheet_name)
        buf.seek(0)
        response = FileResponse(
            buf,
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    else:
        return JsonResponse({"error": "Unsupported template type."}, status=400)

    response["Content-Disposition"] = f'attachment; filename="{filename}"'
    return response


def download_calculated_data(request, file_format):
    """Download the calculated/merged dashboard data as CSV or Excel.

    Uses the same filters currently applied on the dashboard.
    The export mirrors the logic from scripts/cleaning_mapping_merging.py.
    """
    from apps.dashboard.services.export_services import export_csv, export_excel
    from datetime import datetime

    user = get_logged_in_user(request)
    if not user:
        return redirect("account-login")

    filters = build_filters_from_querydict(request.GET)

    # Optional export override:
    # If dashboard platform filter is "All", frontend can pass export_platform=Amazon|Flipkart
    # to force a platform-specific export schema/calculation set.
    export_platform = (request.GET.get("export_platform") or "").strip()
    if export_platform in {"Amazon", "Flipkart"}:
        filters["platform"] = export_platform

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    if file_format == "csv":
        buf = export_csv(user, filters)
        response = FileResponse(buf, content_type="text/csv")
        response["Content-Disposition"] = (
            f'attachment; filename="Calculated_Dashboard_Data_{timestamp}.csv"'
        )
        return response
    elif file_format == "excel":
        buf = export_excel(user, filters)
        response = FileResponse(
            buf,
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        response["Content-Disposition"] = (
            f'attachment; filename="Calculated_Dashboard_Data_{timestamp}.xlsx"'
        )
        return response
    else:
        from django.http import JsonResponse

        return JsonResponse(
            {"error": "Invalid format. Use 'csv' or 'excel'."}, status=400
        )


@require_GET
def download_fsn_status_revenue(request):
    """Download the FSN status revenue detail rows behind the KPI card."""
    from apps.dashboard.services.export_services import export_fsn_status_revenue_csv
    from datetime import datetime

    user = get_logged_in_user(request)
    if not user:
        return redirect("account-login")

    status_key = (request.GET.get("status") or "").strip().lower()
    filters = build_filters_from_querydict(request.GET)

    try:
        buf = export_fsn_status_revenue_csv(user, filters, status_key)
    except ValueError as exc:
        return JsonResponse({"error": str(exc)}, status=400)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_status = status_key if status_key in {"continued", "discontinued", "unmapped"} else "fsn"
    response = FileResponse(buf, content_type="text/csv")
    response["Content-Disposition"] = (
        f'attachment; filename="FSN_Status_Revenue_{safe_status}_{timestamp}.csv"'
    )
    return response
