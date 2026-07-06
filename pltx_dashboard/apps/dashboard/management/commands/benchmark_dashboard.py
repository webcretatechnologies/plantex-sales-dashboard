import json
import time
from types import SimpleNamespace

from django.core.cache import cache
from django.core.management.base import BaseCommand, CommandError
from django.test import RequestFactory

from apps.accounts.models import Users
from apps.dashboard.services.filters import LIST_FILTER_FIELDS
from apps.dashboard.views import get_dashboard_context


SECTION_CONFIG = {
    "initial": {"include_payload": False, "section_scope": "all", "compute_scope": "full"},
    "overview": {"include_payload": True, "section_scope": "overview", "compute_scope": "kpis"},
    "visuals": {"include_payload": True, "section_scope": "visuals", "compute_scope": "full"},
    "details": {"include_payload": True, "section_scope": "details", "compute_scope": "full"},
}


def _parse_filter_args(items):
    filters = {}
    for item in items or []:
        if "=" not in item:
            raise CommandError(f"Invalid --filter value '{item}'. Expected key=value.")
        key, value = item.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            raise CommandError("Filter keys cannot be empty.")
        if key in LIST_FILTER_FIELDS:
            filters[key] = [part.strip() for part in value.split(",") if part.strip()]
        else:
            filters[key] = value
    return filters


def _bump_data_version(user_id):
    key = f"dashboard_data_version_{user_id}"
    try:
        cache.add(key, 0, timeout=None)
        return cache.incr(key)
    except Exception:
        current = cache.get(key, 0) or 0
        new_value = int(current) + 1
        cache.set(key, new_value, timeout=None)
        return new_value


def _payload_stats(payload):
    if not isinstance(payload, dict):
        return {"payload": False}
    inventory = payload.get("inventory") or {}
    return {
        "payload": True,
        "cache_status": payload.get("_cache_status") or "fresh",
        "compute_scope": payload.get("_compute_scope") or "",
        "category_rows": len(payload.get("category_performance") or []),
        "cluster_rows": len(payload.get("cluster_performance") or []),
        "top_products": len(payload.get("cat_top_products") or []),
        "under_products": len(payload.get("cat_under_products") or []),
        "npd_products": len(payload.get("npd_products") or []),
        "inventory_details": len(inventory.get("details") or []),
    }


class Command(BaseCommand):
    help = "Benchmark dashboard context generation against the local database."

    def add_arguments(self, parser):
        parser.add_argument("--user-id", type=int, default=1, help="Data-owner user id.")
        parser.add_argument(
            "--view",
            choices=["business", "ceo", "category"],
            default="business",
            help="Dashboard view to benchmark.",
        )
        parser.add_argument(
            "--sections",
            default="initial,overview,visuals,details",
            help="Comma-separated sections: initial, overview, visuals, details.",
        )
        parser.add_argument(
            "--filter",
            action="append",
            default=[],
            help="Dashboard filter as key=value. Repeat for multiple filters.",
        )
        parser.add_argument("--runs", type=int, default=1, help="Number of measured runs.")
        parser.add_argument(
            "--cold",
            action="store_true",
            help="Bump the dashboard data version before the first run.",
        )
        parser.add_argument(
            "--json",
            action="store_true",
            help="Print machine-readable JSON.",
        )

    def handle(self, *args, **options):
        user = Users.objects.filter(id=options["user_id"]).first()
        if not user:
            raise CommandError(f"User id {options['user_id']} not found.")

        sections = [part.strip() for part in options["sections"].split(",") if part.strip()]
        unknown = [section for section in sections if section not in SECTION_CONFIG]
        if unknown:
            raise CommandError(f"Unknown section(s): {', '.join(unknown)}")

        filters = _parse_filter_args(options["filter"])
        if options["cold"]:
            _bump_data_version(user.id)

        factory = RequestFactory()
        view_type = f"{options['view']}-dashboard"
        results = []

        for run_number in range(1, max(int(options["runs"] or 1), 1) + 1):
            for section in sections:
                config = SECTION_CONFIG[section]
                request = factory.get(f"/dashboard/{options['view']}/", data=filters)
                request.session = {"user_id": user.id}
                request.resolver_match = SimpleNamespace(
                    url_name=view_type,
                    kwargs={"view_name": options["view"]},
                )

                started = time.perf_counter()
                ctx = get_dashboard_context(
                    request,
                    include_payload=config["include_payload"],
                    cache_view_type=view_type,
                    section_scope=config["section_scope"],
                    compute_scope=config["compute_scope"],
                )
                elapsed = time.perf_counter() - started
                payload = (ctx or {}).get("payload")

                result = {
                    "run": run_number,
                    "section": section,
                    "seconds": round(elapsed, 3),
                    "user_id": user.id,
                    "view": options["view"],
                    "filters": filters,
                }
                result.update(_payload_stats(payload))
                results.append(result)

                if not options["json"]:
                    self.stdout.write(
                        f"run={run_number} section={section} "
                        f"seconds={result['seconds']} status={result.get('cache_status')}"
                    )

        if options["json"]:
            self.stdout.write(json.dumps(results, indent=2, default=str))
