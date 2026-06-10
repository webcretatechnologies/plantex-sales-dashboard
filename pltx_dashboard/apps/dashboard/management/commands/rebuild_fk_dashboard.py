"""
Management command: rebuild_fk_dashboard

Force-rebuilds FlipkartProcessedDashboardData from the raw
FlipkartSearchTraffic and FlipkartPLA tables.

Usage:
    # Full rebuild for all users
    python manage.py rebuild_fk_dashboard

    # Rebuild for a specific user (by ID or email)
    python manage.py rebuild_fk_dashboard --user 1
    python manage.py rebuild_fk_dashboard --user admin@example.com

    # Rebuild only specific dates
    python manage.py rebuild_fk_dashboard --dates 2026-04-25 2026-04-28

    # Rebuild for a user + specific dates
    python manage.py rebuild_fk_dashboard --user 1 --dates 2026-04-25 2026-04-28
"""

import logging

from django.core.management.base import BaseCommand, CommandError

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = (
        "Force-rebuild FlipkartProcessedDashboardData from raw FlipkartSearchTraffic "
        "and FlipkartPLA tables. Use --dates to limit to specific dates."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--user",
            type=str,
            default=None,
            help="User ID or email of the data owner. Rebuilds all users if omitted.",
        )
        parser.add_argument(
            "--dates",
            nargs="*",
            default=None,
            help=(
                "One or more dates in YYYY-MM-DD format to rebuild. "
                "Rebuilds ALL dates if omitted (full rebuild)."
            ),
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            default=False,
            help="Print what would be done without actually running the rebuild.",
        )

    def handle(self, *args, **options):
        from apps.accounts.models import Users
        from apps.dashboard.models import FlipkartSearchTraffic
        from apps.upload.dashboard_builders import generate_flipkart_dashboard_data
        from apps.dashboard.services.invalidation import invalidate_dashboard_cache_for_user

        user_arg = options["user"]
        date_args = options["dates"]
        dry_run = options["dry_run"]

        # ------------------------------------------------------------------ #
        # Resolve target users
        # ------------------------------------------------------------------ #
        if user_arg:
            if user_arg.isdigit():
                try:
                    target_users = [Users.objects.get(pk=int(user_arg))]
                except Users.DoesNotExist:
                    raise CommandError(f"User with ID {user_arg} not found.")
            else:
                try:
                    target_users = [Users.objects.get(email=user_arg)]
                except Users.DoesNotExist:
                    raise CommandError(f"User with email '{user_arg}' not found.")
        else:
            # All users that have any FK search traffic data
            user_ids = (
                FlipkartSearchTraffic.objects
                .values_list("user_id", flat=True)
                .distinct()
            )
            target_users = list(Users.objects.filter(pk__in=user_ids))
            if not target_users:
                self.stdout.write(self.style.WARNING("No users with FK Search Traffic data found."))
                return

        self.stdout.write(
            self.style.HTTP_INFO(
                f"Found {len(target_users)} user(s) to rebuild: "
                f"{[u.email for u in target_users]}"
            )
        )

        # ------------------------------------------------------------------ #
        # Validate / parse target dates
        # ------------------------------------------------------------------ #
        import datetime
        only_dates = None
        if date_args:
            only_dates = []
            for ds in date_args:
                try:
                    only_dates.append(
                        datetime.datetime.strptime(ds.strip(), "%Y-%m-%d").date()
                    )
                except ValueError:
                    raise CommandError(
                        f"Invalid date '{ds}'. Expected format: YYYY-MM-DD"
                    )
            self.stdout.write(
                self.style.HTTP_INFO(
                    f"Limiting rebuild to dates: {[str(d) for d in only_dates]}"
                )
            )
        else:
            self.stdout.write(
                self.style.WARNING("No --dates specified — performing FULL rebuild for all dates.")
            )

        if dry_run:
            self.stdout.write(self.style.WARNING("DRY RUN — no changes will be made."))
            return

        # ------------------------------------------------------------------ #
        # Run rebuild per user
        # ------------------------------------------------------------------ #
        for user in target_users:
            self.stdout.write(f"\nProcessing user: {user.email} (id={user.id})")

            # Check if raw traffic data exists
            traffic_qs = FlipkartSearchTraffic.objects.filter(user=user)
            if only_dates:
                traffic_qs = traffic_qs.filter(date__in=only_dates)

            if not traffic_qs.exists():
                self.stdout.write(
                    self.style.WARNING(
                        f"  ⚠ No FlipkartSearchTraffic data found "
                        f"{'for specified dates' if only_dates else ''}. Skipping."
                    )
                )
                continue

            dates_in_db = sorted(
                traffic_qs.values_list("date", flat=True).distinct()
            )
            self.stdout.write(
                f"  Found traffic data for {len(dates_in_db)} date(s): "
                f"{[str(d) for d in dates_in_db]}"
            )

            def _progress(msg):
                self.stdout.write(f"    {msg}")

            try:
                generate_flipkart_dashboard_data(
                    user,
                    progress_callback=_progress,
                    only_dates=[str(d) for d in (only_dates or dates_in_db)],
                )
                invalidate_dashboard_cache_for_user(user.id, clear_materialized=True)
                self.stdout.write(
                    self.style.SUCCESS(
                        f"  ✓ Rebuild complete for user {user.email}. Cache invalidated."
                    )
                )
            except ValueError as exc:
                self.stdout.write(
                    self.style.ERROR(
                        f"  ✗ Rebuild skipped for user {user.email}: {exc}\n"
                        f"    This usually means Search Traffic, Category, PLA or Price "
                        f"data is missing. Upload all required FK files first."
                    )
                )
            except Exception as exc:
                self.stdout.write(
                    self.style.ERROR(f"  ✗ Rebuild FAILED for user {user.email}: {exc}")
                )
                logger.exception("[rebuild_fk_dashboard] Error for user=%s", user.id)

        self.stdout.write(self.style.SUCCESS("\nDone."))
