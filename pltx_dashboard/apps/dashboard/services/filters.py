import json
import datetime

from django.utils import timezone
from django.db.models import Q


LARGE_IN_FILTER_CHUNK_SIZE = 1000
LIST_FILTER_FIELDS = {
    "category",
    "asin",
    "fsn",
    "portfolio",
    "subcategory",
    "category_manager",
    "series_name",
    "material",
    "size",
    "brand_name",
    "ratings",
    "finish",
    "inventory_health",
    "parent_asin",
    "sku",
}
MAPPING_FILTER_FIELDS = {
    "category",
    "portfolio",
    "subcategory",
    "category_manager",
    "series_name",
    "material",
    "size",
    "brand_name",
    "ratings",
    "finish",
}
PAYLOAD_FILTER_FIELDS = [
    "date_range",
    "start_date",
    "end_date",
    "compare_start_date",
    "compare_end_date",
    "platform",
    "category",
    "asin",
    "fsn",
    "portfolio",
    "subcategory",
    "category_manager",
    "series_name",
    "material",
    "size",
    "brand_name",
    "ratings",
    "parent_asin",
    "finish",
    "inventory_health",
    "sku",
    "year",
    "launch_date_range",
    "launch_start_date",
    "launch_end_date",
]


def build_filters_from_querydict(querydict):
    filters = {}
    for key in querydict.keys():
        if key in LIST_FILTER_FIELDS:
            filters[key] = [value for value in querydict.getlist(key) if value]
        else:
            filters[key] = querydict.get(key, "")
    _normalize_launch_date_filters(filters)
    return filters


def _normalize_launch_date_filters(filters):
    if str(filters.get("launch_date_range") or "").strip() != "custom":
        return filters

    start = str(filters.get("launch_start_date") or "").strip()
    end = str(filters.get("launch_end_date") or "").strip()
    if start and not end:
        filters["launch_end_date"] = start
    elif end and not start:
        filters["launch_start_date"] = end

    launch_start = _parse_date(filters.get("launch_start_date"))
    launch_end = _parse_date(filters.get("launch_end_date"))
    if launch_start and launch_end and launch_end < launch_start:
        filters["launch_start_date"], filters["launch_end_date"] = (
            filters["launch_end_date"],
            filters["launch_start_date"],
        )
    return filters


def selected_filter_payload(filters):
    return {
        "categories": filters.get("category", []),
        "asins": filters.get("asin", []),
        "fsns": filters.get("fsn", []),
        "portfolios": filters.get("portfolio", []),
        "subcategories": filters.get("subcategory", []),
        "category_managers": filters.get("category_manager", []),
        "series_names": filters.get("series_name", []),
        "materials": filters.get("material", []),
        "sizes": filters.get("size", []),
        "brand_names": filters.get("brand_name", []),
        "ratings": filters.get("ratings", []),
        "parent_asins": filters.get("parent_asin", []),
        "finishes": filters.get("finish", []),
        "inventory_health_filters": filters.get("inventory_health", []),
        "skus": filters.get("sku", []),
        "launch_date_range": filters.get("launch_date_range", ""),
        "launch_start_date": filters.get("launch_start_date", ""),
        "launch_end_date": filters.get("launch_end_date", ""),
    }


def _apply_value_filter(qs, field_name, value):
    if not value:
        return qs
    if isinstance(value, (list, tuple, set)):
        values = [str(item) for item in value if str(item).strip()]
        return _apply_in_filter(qs, field_name, values) if values else qs
    return qs.filter(**{field_name: value})


def _apply_in_filter(qs, field_name, values, chunk_size=LARGE_IN_FILTER_CHUNK_SIZE):
    values = [str(item).strip() for item in (values or []) if str(item).strip()]
    if not values:
        return qs.none()
    if len(values) <= chunk_size:
        return qs.filter(**{f"{field_name}__in": values})

    predicate = Q()
    for idx in range(0, len(values), chunk_size):
        predicate |= Q(**{f"{field_name}__in": values[idx : idx + chunk_size]})
    return qs.filter(predicate)


def _has_mapping_filters(filters):
    return bool(filters.get("parent_asin") or filters.get("sku") or any(filters.get(field) for field in MAPPING_FILTER_FIELDS))


def _mapping_values(value):
    if not value:
        return []
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    item = str(value).strip()
    return [item] if item else []


def _apply_mapping_filter(qs, field_name, value):
    values = _mapping_values(value)
    if not values:
        return qs
        
    if field_name == "ratings":
        allowed_ratings = set()
        for rating_str in qs.exclude(ratings__isnull=True).exclude(ratings="").values_list("ratings", flat=True).distinct():
            try:
                v = float(rating_str)
                for r in values:
                    if r == "0 - 1" and 0 <= v < 1:
                        allowed_ratings.add(rating_str)
                    elif r == "1 - 3" and 1 <= v < 3:
                        allowed_ratings.add(rating_str)
                    elif r == "3 - 3.5" and 3 <= v < 3.5:
                        allowed_ratings.add(rating_str)
                    elif r == "3.5 - 4" and 3.5 <= v < 4:
                        allowed_ratings.add(rating_str)
                    elif r == "4 - 4.2" and 4 <= v < 4.2:
                        allowed_ratings.add(rating_str)
                    elif r == "4.2 - 4.5" and 4.2 <= v < 4.5:
                        allowed_ratings.add(rating_str)
                    elif r == "4.5+" and v >= 4.5:
                        allowed_ratings.add(rating_str)
            except (ValueError, TypeError):
                pass
        return qs.filter(ratings__in=allowed_ratings) if allowed_ratings else qs.none()
        
    return qs.filter(**{f"{field_name}__in": values})


def _mapping_allow_lists(user, filters):
    launch_start, launch_end = _resolve_launch_date_bounds(filters)
    if not user or (not _has_mapping_filters(filters) and not (launch_start or launch_end)):
        return None, None

    from apps.dashboard.models import CategoryMapping, FlipkartCategoryMap

    az_map = CategoryMapping.objects.filter(user=user)
    fk_map = FlipkartCategoryMap.objects.filter(user=user)

    if launch_start or launch_end:
        az_map = az_map.exclude(launch_date__isnull=True)
        fk_map = fk_map.exclude(launch_date__isnull=True)
        if launch_start:
            az_map = az_map.filter(launch_date__gte=launch_start)
            fk_map = fk_map.filter(launch_date__gte=launch_start)
        if launch_end:
            az_map = az_map.filter(launch_date__lte=launch_end)
            fk_map = fk_map.filter(launch_date__lte=launch_end)
    for field in MAPPING_FILTER_FIELDS:
        az_map = _apply_mapping_filter(az_map, field, filters.get(field))
        fk_map = _apply_mapping_filter(fk_map, field, filters.get(field))

    parent_values = _mapping_values(filters.get("parent_asin"))
    if parent_values:
        child_asins = set(
            CategoryMapping.objects.filter(
                user=user,
                parent_asin__in=parent_values,
            ).values_list("asin", flat=True)
        )
        az_map = az_map.filter(asin__in=child_asins)
        fk_map = fk_map.filter(asin__in=child_asins)

    sku_values = _mapping_values(filters.get("sku"))
    if sku_values:
        az_map = az_map.filter(msku__in=sku_values)
        fk_map = fk_map.filter(sku__in=sku_values)

    return (
        list(az_map.values_list("asin", flat=True).distinct()),
        list(fk_map.values_list("fsn", flat=True).distinct()),
    )


def launch_date_allow_lists(user, filters):
    launch_start, launch_end = _resolve_launch_date_bounds(filters)
    if not user or not (launch_start or launch_end):
        return None, None

    from apps.dashboard.models import CategoryMapping, FlipkartCategoryMap

    az_map = CategoryMapping.objects.filter(user=user).exclude(launch_date__isnull=True)
    fk_map = FlipkartCategoryMap.objects.filter(user=user).exclude(launch_date__isnull=True)
    if launch_start:
        az_map = az_map.filter(launch_date__gte=launch_start)
        fk_map = fk_map.filter(launch_date__gte=launch_start)
    if launch_end:
        az_map = az_map.filter(launch_date__lte=launch_end)
        fk_map = fk_map.filter(launch_date__lte=launch_end)
    return (
        list(az_map.values_list("asin", flat=True).distinct()),
        list(fk_map.values_list("fsn", flat=True).distinct()),
    )


def _parse_date(value):
    if not value:
        return None
    try:
        return datetime.datetime.strptime(str(value).strip(), "%Y-%m-%d").date()
    except (TypeError, ValueError):
        return None


def _resolve_launch_date_bounds(filters):
    date_range = str(filters.get("launch_date_range") or "").strip()
    today = timezone.localdate()
    start = end = None
    if date_range == "lt_1_month":
        start, end = today - datetime.timedelta(days=30), today
    elif date_range == "lt_3_months":
        start, end = today - datetime.timedelta(days=90), today
    elif date_range == "lt_6_months":
        start, end = today - datetime.timedelta(days=180), today
    elif date_range == "custom":
        start = _parse_date(filters.get("launch_start_date"))
        end = _parse_date(filters.get("launch_end_date"))
        if start and not end:
            end = start
        elif end and not start:
            start = end
        if start and end and end < start:
            start, end = end, start
    return start, end


def has_launch_date_filter(filters):
    launch_start, launch_end = _resolve_launch_date_bounds(filters)
    return bool(launch_start or launch_end)


def apply_dashboard_entity_filters(qs, fk_qs, filters, user=None):
    platform = filters.get("platform")
    show_amazon = platform != "Flipkart"
    show_flipkart = platform != "Amazon"

    qs = _apply_value_filter(qs, "category", filters.get("category"))
    fk_qs = _apply_value_filter(fk_qs, "category", filters.get("category"))

    asin_filter = filters.get("asin")
    fsn_filter = filters.get("fsn")
    qs = _apply_value_filter(qs, "asin", asin_filter)
    fk_qs = _apply_value_filter(fk_qs, "fsn", fsn_filter)

    if asin_filter and not fsn_filter:
        fk_qs = fk_qs.none()
    elif fsn_filter and not asin_filter:
        qs = qs.none()

    qs = _apply_value_filter(qs, "portfolio", filters.get("portfolio"))
    fk_qs = _apply_value_filter(fk_qs, "portfolio", filters.get("portfolio"))
    qs = _apply_value_filter(qs, "subcategory", filters.get("subcategory"))
    fk_qs = _apply_value_filter(fk_qs, "subcategory", filters.get("subcategory"))

    allowed_asins, allowed_fsns = _mapping_allow_lists(user, filters)
    if allowed_asins is not None:
        qs = _apply_in_filter(qs, "asin", allowed_asins) if allowed_asins else qs.none()
    if allowed_fsns is not None:
        fk_qs = _apply_in_filter(fk_qs, "fsn", allowed_fsns) if allowed_fsns else fk_qs.none()

    inv_health_vals = _mapping_values(filters.get("inventory_health"))
    if inv_health_vals and user:
        from apps.dashboard.models import DashboardInventoryHealthSummary
        from django.db.models import Max
        max_date = DashboardInventoryHealthSummary.objects.filter(user=user).aggregate(Max('date'))['date__max']
        if max_date:
            fk_statuses = []
            for status in inv_health_vals:
                if status == "In Stock":
                    fk_statuses.append("Ideal Stocking")
                elif status == "Low Stock":
                    fk_statuses.append("Understock")
                elif status == "OOS":
                    fk_statuses.append("OOS")
                    fk_statuses.append("Nearly OOS")
                elif status == "Overstock":
                    fk_statuses.append("Over Stock")
                    fk_statuses.append("Highly Over Stock")
                    fk_statuses.append("Not Selling")

            # Amazon: filter by Amazon/Combined rows, exclude null ASINs
            allowed_asins_inv = set(
                DashboardInventoryHealthSummary.objects.filter(
                    user=user,
                    date=max_date,
                    platform__in=["Amazon", "Combined"],
                    status__in=inv_health_vals,
                )
                .exclude(asin__isnull=True)
                .exclude(asin="")
                .values_list("asin", flat=True)
            )
            # Flipkart: filter by Flipkart/Combined rows, exclude null FSNs
            allowed_fsns_inv = set(
                DashboardInventoryHealthSummary.objects.filter(
                    user=user,
                    date=max_date,
                    platform__in=["Flipkart", "Combined"],
                    fk_status__in=fk_statuses,
                )
                .exclude(fsn__isnull=True)
                .exclude(fsn="")
                .values_list("fsn", flat=True)
            )

            qs = _apply_in_filter(qs, "asin", allowed_asins_inv) if allowed_asins_inv else qs.none()
            fk_qs = _apply_in_filter(fk_qs, "fsn", allowed_fsns_inv) if allowed_fsns_inv else fk_qs.none()
        else:
            qs = qs.none()
            fk_qs = fk_qs.none()

    if not show_amazon:
        qs = qs.none()
    if not show_flipkart:
        fk_qs = fk_qs.none()

    return qs, fk_qs


def get_filtered_mapping_querysets(filters, user):
    from apps.dashboard.models import CategoryMapping, FlipkartCategoryMap

    az_map = CategoryMapping.objects.filter(user=user)
    fk_map = FlipkartCategoryMap.objects.filter(user=user)

    platform = filters.get("platform")
    show_amazon = platform != "Flipkart"
    show_flipkart = platform != "Amazon"

    az_map = _apply_value_filter(az_map, "category", filters.get("category"))
    fk_map = _apply_value_filter(fk_map, "category", filters.get("category"))

    asin_filter = filters.get("asin")
    fsn_filter = filters.get("fsn")
    az_map = _apply_value_filter(az_map, "asin", asin_filter)
    fk_map = _apply_value_filter(fk_map, "fsn", fsn_filter)

    if asin_filter and not fsn_filter:
        fk_map = fk_map.none()
    elif fsn_filter and not asin_filter:
        az_map = az_map.none()

    az_map = _apply_value_filter(az_map, "portfolio", filters.get("portfolio"))
    fk_map = _apply_value_filter(fk_map, "portfolio", filters.get("portfolio"))
    az_map = _apply_value_filter(az_map, "subcategory", filters.get("subcategory"))
    fk_map = _apply_value_filter(fk_map, "subcategory", filters.get("subcategory"))

    launch_start, launch_end = _resolve_launch_date_bounds(filters)
    if launch_start or launch_end:
        az_map = az_map.exclude(launch_date__isnull=True)
        fk_map = fk_map.exclude(launch_date__isnull=True)
        if launch_start:
            az_map = az_map.filter(launch_date__gte=launch_start)
            fk_map = fk_map.filter(launch_date__gte=launch_start)
        if launch_end:
            az_map = az_map.filter(launch_date__lte=launch_end)
            fk_map = fk_map.filter(launch_date__lte=launch_end)

    for field in MAPPING_FILTER_FIELDS:
        if field in {"category", "portfolio", "subcategory"}:
            continue
        az_map = _apply_mapping_filter(az_map, field, filters.get(field))
        fk_map = _apply_mapping_filter(fk_map, field, filters.get(field))

    parent_values = _mapping_values(filters.get("parent_asin"))
    if parent_values:
        child_asins = set(
            CategoryMapping.objects.filter(
                user=user,
                parent_asin__in=parent_values,
            ).values_list("asin", flat=True)
        )
        az_map = az_map.filter(asin__in=child_asins)
        fk_map = fk_map.filter(asin__in=child_asins)

    sku_values = _mapping_values(filters.get("sku"))
    if sku_values:
        az_map = az_map.filter(msku__in=sku_values)
        fk_map = fk_map.filter(sku__in=sku_values)

    inv_health_vals = _mapping_values(filters.get("inventory_health"))
    if inv_health_vals and user:
        from apps.dashboard.models import DashboardInventoryHealthSummary
        from django.db.models import Max
        max_date = DashboardInventoryHealthSummary.objects.filter(user=user).aggregate(Max('date'))['date__max']
        if max_date:
            fk_statuses = []
            for status in inv_health_vals:
                if status == "In Stock":
                    fk_statuses.append("Ideal Stocking")
                elif status == "Low Stock":
                    fk_statuses.append("Understock")
                elif status == "OOS":
                    fk_statuses.append("OOS")
                    fk_statuses.append("Nearly OOS")
                elif status == "Overstock":
                    fk_statuses.append("Over Stock")
                    fk_statuses.append("Highly Over Stock")
                    fk_statuses.append("Not Selling")

            allowed_asins_inv = set(
                DashboardInventoryHealthSummary.objects.filter(
                    user=user,
                    date=max_date,
                    platform__in=["Amazon", "Combined"],
                    status__in=inv_health_vals,
                )
                .exclude(asin__isnull=True)
                .exclude(asin="")
                .values_list("asin", flat=True)
            )
            allowed_fsns_inv = set(
                DashboardInventoryHealthSummary.objects.filter(
                    user=user,
                    date=max_date,
                    platform__in=["Flipkart", "Combined"],
                    fk_status__in=fk_statuses,
                )
                .exclude(fsn__isnull=True)
                .exclude(fsn="")
                .values_list("fsn", flat=True)
            )

            az_map = _apply_in_filter(az_map, "asin", allowed_asins_inv) if allowed_asins_inv else az_map.none()
            fk_map = _apply_in_filter(fk_map, "fsn", allowed_fsns_inv) if allowed_fsns_inv else fk_map.none()
        else:
            az_map = az_map.none()
            fk_map = fk_map.none()

    if not show_amazon:
        az_map = az_map.none()
    if not show_flipkart:
        fk_map = fk_map.none()

    return az_map, fk_map



def normalize_payload_filters(filters):
    cache_filters = {}
    for field in PAYLOAD_FILTER_FIELDS:
        value = filters.get(field)
        if isinstance(value, (list, tuple, set)):
            cache_filters[field] = sorted(
                {str(item) for item in value if str(item).strip()}
            )
        elif value is None:
            cache_filters[field] = ""
        else:
            cache_filters[field] = str(value)
    return cache_filters


def cache_filter_string(filters):
    return json.dumps(normalize_payload_filters(filters), sort_keys=True)
