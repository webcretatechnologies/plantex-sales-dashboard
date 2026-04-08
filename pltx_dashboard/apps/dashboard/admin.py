from django.contrib import admin
from .models import SalesData, SpendData, CategoryMapping, PriceData, ProcessedDashboardData

@admin.register(SalesData)
class SalesDataAdmin(admin.ModelAdmin):
    list_display = ('date', 'asin', 'revenue', 'orders')
    list_filter = ('date', 'asin')
    search_fields = ('asin',)

@admin.register(SpendData)
class SpendDataAdmin(admin.ModelAdmin):
    list_display = ('date', 'asin', 'ad_type', 'spend')
    list_filter = ('date', 'ad_type', 'asin')

@admin.register(CategoryMapping)
class CategoryMappingAdmin(admin.ModelAdmin):
    list_display = ('user', 'asin', 'portfolio', 'category')
    list_filter = ('portfolio', 'category')

@admin.register(PriceData)
class PriceDataAdmin(admin.ModelAdmin):
    list_display = ('user', 'asin', 'price')

@admin.register(ProcessedDashboardData)
class ProcessedDashboardDataAdmin(admin.ModelAdmin):
    list_display = ('user', 'date', 'asin', 'revenue', 'total_spend')
    list_filter = ('date', 'user')
