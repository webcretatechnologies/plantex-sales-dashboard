from django.contrib import admin
from .models import (
    SalesData, SpendData, CategoryMapping, PriceData, ProcessedDashboardData,
    FlipkartSalesReport, FlipkartCurrentInventoryReport, PCADailyReport, PLAFSNReport
)
from .materialized_models import CeoDashboardCache, BusinessDashboardCache, CategoryDashboardCache, DashboardFilterCache

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

@admin.register(FlipkartSalesReport)
class FlipkartSalesReportAdmin(admin.ModelAdmin):
    list_display = ('user', 'order_id', 'order_item_id', 'order_date', 'final_invoice_amount')
    list_filter = ('order_date', 'user')
    search_fields = ('order_id', 'order_item_id', 'sku')

@admin.register(FlipkartCurrentInventoryReport)
class FlipkartCurrentInventoryReportAdmin(admin.ModelAdmin):
    list_display = ('user', 'warehouse_id', 'sku', 'fsn', 'sales_30d')
    list_filter = ('warehouse_id', 'user')
    search_fields = ('sku', 'fsn', 'listing_id')

@admin.register(PCADailyReport)
class PCADailyReportAdmin(admin.ModelAdmin):
    list_display = ('user', 'date', 'campaign_name', 'ad_group_name', 'direct_revenue')
    list_filter = ('date', 'user', 'campaign_name')
    search_fields = ('campaign_name', 'ad_group_name')

@admin.register(PLAFSNReport)
class PLAFSNReportAdmin(admin.ModelAdmin):
    list_display = ('user', 'campaign_name', 'ad_group_name', 'advertised_fsn_id', 'direct_revenue')
    list_filter = ('user', 'campaign_name')
    search_fields = ('campaign_name', 'ad_group_name', 'advertised_fsn_id')


# ─── Materialized-view cache admin ───

@admin.register(CeoDashboardCache)
class CeoDashboardCacheAdmin(admin.ModelAdmin):
    list_display = ('user', 'refreshed_at')
    readonly_fields = ('user', 'payload_json', 'refreshed_at')

@admin.register(BusinessDashboardCache)
class BusinessDashboardCacheAdmin(admin.ModelAdmin):
    list_display = ('user', 'refreshed_at')
    readonly_fields = ('user', 'payload_json', 'refreshed_at')

@admin.register(CategoryDashboardCache)
class CategoryDashboardCacheAdmin(admin.ModelAdmin):
    list_display = ('user', 'refreshed_at')
    readonly_fields = ('user', 'payload_json', 'refreshed_at')

@admin.register(DashboardFilterCache)
class DashboardFilterCacheAdmin(admin.ModelAdmin):
    list_display = ('user', 'filter_key', 'refreshed_at')
    list_filter = ('filter_key',)
    readonly_fields = ('user', 'filter_key', 'payload_json', 'refreshed_at')

