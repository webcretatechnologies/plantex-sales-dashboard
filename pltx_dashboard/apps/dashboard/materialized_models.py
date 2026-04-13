from django.db import models
from django.utils import timezone


class CeoDashboardCache(models.Model):
    """
    Materialized-view cache for the CEO Dashboard.
    Stores the fully pre-computed payload as a JSON blob per user.
    Refreshed automatically when data is uploaded.
    """
    user = models.OneToOneField(
        'accounts.Users',
        on_delete=models.CASCADE,
        related_name='ceo_dashboard_cache',
        primary_key=True,
    )
    payload_json = models.JSONField(default=dict)
    refreshed_at = models.DateTimeField(default=timezone.now)

    class Meta:
        db_table = 'dashboard_ceo_cache'
        verbose_name = 'CEO Dashboard Cache'
        verbose_name_plural = 'CEO Dashboard Caches'

    def __str__(self):
        return f"CEO Cache — {self.user} (refreshed {self.refreshed_at})"


class BusinessDashboardCache(models.Model):
    """
    Materialized-view cache for the Business Dashboard.
    Stores the fully pre-computed payload as a JSON blob per user.
    Refreshed automatically when data is uploaded.
    """
    user = models.OneToOneField(
        'accounts.Users',
        on_delete=models.CASCADE,
        related_name='business_dashboard_cache',
        primary_key=True,
    )
    payload_json = models.JSONField(default=dict)
    refreshed_at = models.DateTimeField(default=timezone.now)

    class Meta:
        db_table = 'dashboard_business_cache'
        verbose_name = 'Business Dashboard Cache'
        verbose_name_plural = 'Business Dashboard Caches'

    def __str__(self):
        return f"Business Cache — {self.user} (refreshed {self.refreshed_at})"


class CategoryDashboardCache(models.Model):
    """
    Materialized-view cache for the Category Dashboard.
    Stores the fully pre-computed payload as a JSON blob per user.
    Refreshed automatically when data is uploaded.
    """
    user = models.OneToOneField(
        'accounts.Users',
        on_delete=models.CASCADE,
        related_name='category_dashboard_cache',
        primary_key=True,
    )
    payload_json = models.JSONField(default=dict)
    refreshed_at = models.DateTimeField(default=timezone.now)

    class Meta:
        db_table = 'dashboard_category_cache'
        verbose_name = 'Category Dashboard Cache'
        verbose_name_plural = 'Category Dashboard Caches'

    def __str__(self):
        return f"Category Cache — {self.user} (refreshed {self.refreshed_at})"


# Standard date-range filter keys that we pre-compute
STANDARD_DATE_RANGES = [
    'yesterday', 'last_7_days', 'last_15_days',
    'last_month', 'last_3_months', 'last_6_months', 'last_1_year',
]


class DashboardFilterCache(models.Model):
    """
    Materialized-view cache for date-range filtered dashboard views.
    One row per (user, filter_key) combination.  Pre-computed during
    the upload pipeline so that common filter selections are instant.
    """
    user = models.ForeignKey(
        'accounts.Users',
        on_delete=models.CASCADE,
        related_name='dashboard_filter_caches',
    )
    filter_key = models.CharField(
        max_length=50,
        help_text='Standard date range key, e.g. "last_7_days", "last_3_months".',
    )
    payload_json = models.JSONField(default=dict)
    refreshed_at = models.DateTimeField(default=timezone.now)

    class Meta:
        db_table = 'dashboard_filter_cache'
        unique_together = ('user', 'filter_key')
        verbose_name = 'Dashboard Filter Cache'
        verbose_name_plural = 'Dashboard Filter Caches'

    def __str__(self):
        return f"Filter Cache — {self.user} / {self.filter_key} (refreshed {self.refreshed_at})"

