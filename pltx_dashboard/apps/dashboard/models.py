from django.db import models

class SalesData(models.Model):
    user = models.ForeignKey('accounts.Users', on_delete=models.CASCADE, related_name='sales_records')
    date = models.DateField(db_index=True)
    asin = models.CharField(max_length=50, db_index=True)
    pageviews = models.IntegerField(default=0)
    units = models.IntegerField(default=0)
    orders = models.IntegerField(default=0)
    revenue = models.FloatField(default=0.0)

    class Meta:
        unique_together = ('user', 'date', 'asin')

class SpendData(models.Model):
    user = models.ForeignKey('accounts.Users', on_delete=models.CASCADE, related_name='spend_records')
    date = models.DateField(db_index=True)
    asin = models.CharField(max_length=50, db_index=True)
    ad_account = models.CharField(max_length=100)
    ad_type = models.CharField(max_length=10)
    spend = models.FloatField(default=0.0)

    class Meta:
        unique_together = ('user', 'date', 'asin', 'ad_account', 'ad_type')

class CategoryMapping(models.Model):
    user = models.ForeignKey('accounts.Users', on_delete=models.CASCADE, related_name='category_mappings')
    asin = models.CharField(max_length=50, db_index=True)
    portfolio = models.CharField(max_length=100)
    category = models.CharField(max_length=100)
    subcategory = models.CharField(max_length=100)

    class Meta:
        unique_together = ('user', 'asin')

class PriceData(models.Model):
    user = models.ForeignKey('accounts.Users', on_delete=models.CASCADE, related_name='price_data')
    asin = models.CharField(max_length=50, db_index=True)
    price = models.FloatField(default=0.0)

    class Meta:
        unique_together = ('user', 'asin')

class ProcessedDashboardData(models.Model):
    user = models.ForeignKey('accounts.Users', on_delete=models.CASCADE, related_name='processed_dashboard_data')
    date = models.DateField(db_index=True)
    asin = models.CharField(max_length=50, db_index=True)
    portfolio = models.CharField(max_length=100, db_index=True, null=True, blank=True)
    category = models.CharField(max_length=100, db_index=True, null=True, blank=True)
    subcategory = models.CharField(max_length=100, db_index=True, null=True, blank=True)
    price = models.FloatField(default=0.0)
    
    pageviews = models.IntegerField(default=0)
    units = models.IntegerField(default=0)
    orders = models.IntegerField(default=0)
    revenue = models.FloatField(default=0.0)
    
    spend_sp = models.FloatField(default=0.0)
    spend_sb = models.FloatField(default=0.0)
    spend_sd = models.FloatField(default=0.0)
    total_spend = models.FloatField(default=0.0)

    class Meta:
        unique_together = ('user', 'date', 'asin')
