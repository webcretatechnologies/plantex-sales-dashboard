"""
Management command to refresh the materialized-view dashboard caches.

Usage:
    python manage.py refresh_dashboard_cache              # All main users
    python manage.py refresh_dashboard_cache --user 5     # Specific user by ID
"""

from django.core.management.base import BaseCommand

from apps.accounts.models import Users
from apps.dashboard.services.materialized_services import refresh_materialized_views


class Command(BaseCommand):
    help = 'Refresh the materialized-view caches for CEO, Business & Category dashboards.'

    def add_arguments(self, parser):
        parser.add_argument(
            '--user',
            type=int,
            default=None,
            help='Refresh cache for a specific user ID only.',
        )

    def handle(self, *args, **options):
        user_id = options['user']

        if user_id:
            try:
                user = Users.objects.get(id=user_id)
            except Users.DoesNotExist:
                self.stderr.write(self.style.ERROR(f'User with id={user_id} does not exist.'))
                return
            self._refresh_for_user(user)
        else:
            # Refresh for all main (data-owning) users
            main_users = Users.objects.filter(role__name__iexact='admin')
            if not main_users.exists():
                self.stdout.write(self.style.WARNING('No main users found.'))
                return

            for user in main_users:
                self._refresh_for_user(user)

        self.stdout.write(self.style.SUCCESS('Done.'))

    def _refresh_for_user(self, user):
        self.stdout.write(f'Refreshing caches for user {user} (id={user.pk})...')
        try:
            refresh_materialized_views(user)
            self.stdout.write(self.style.SUCCESS(f'  ✓ {user}'))
        except Exception as exc:
            self.stderr.write(self.style.ERROR(f'  ✗ {user}: {exc}'))
