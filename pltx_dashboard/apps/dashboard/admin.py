from django.contrib import admin
from .models import (
    SalesData, SpendData, CategoryMapping, PriceData, ProcessedDashboardData,
    FlipkartSearchTraffic, FlipkartCategoryMap, FlipkartPrice,
    FlipkartPCA, FlipkartPLA, FlipkartSalesInvoice, FlipkartCoupon,
    FlipkartProcessedDashboardData
)

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

# ─── Slim Flipkart Models Admin ───

@admin.register(FlipkartSearchTraffic)
class FlipkartSearchTrafficAdmin(admin.ModelAdmin):
    list_display = ('user', 'date', 'fsn', 'sku', 'revenue')
    list_filter = ('date', 'user')
    search_fields = ('fsn', 'sku')

@admin.register(FlipkartCategoryMap)
class FlipkartCategoryMapAdmin(admin.ModelAdmin):
    list_display = ('user', 'fsn', 'portfolio', 'category', 'subcategory')
    list_filter = ('portfolio', 'category')
    search_fields = ('fsn', 'sku')

@admin.register(FlipkartPrice)
class FlipkartPriceAdmin(admin.ModelAdmin):
    list_display = ('user', 'fsn', 'price')
    search_fields = ('fsn',)

@admin.register(FlipkartPCA)
class FlipkartPCAAdmin(admin.ModelAdmin):
    list_display = ('user', 'date', 'campaign_name', 'fsn_id')
    list_filter = ('date', 'user')
    search_fields = ('campaign_id', 'campaign_name', 'fsn_id')

@admin.register(FlipkartPLA)
class FlipkartPLAAdmin(admin.ModelAdmin):
    list_display = ('user', 'campaign_id', 'fsn_id', 'ad_spend')
    search_fields = ('campaign_id', 'fsn_id')

@admin.register(FlipkartSalesInvoice)
class FlipkartSalesInvoiceAdmin(admin.ModelAdmin):
    list_display = ('user', 'order_id', 'order_item_id', 'fsn', 'invoice_amount')
    search_fields = ('order_id', 'order_item_id', 'fsn')

@admin.register(FlipkartCoupon)
class FlipkartCouponAdmin(admin.ModelAdmin):
    list_display = ('user', 'fsn', 'coupon_value')
    search_fields = ('fsn',)

@admin.register(FlipkartProcessedDashboardData)
class FlipkartProcessedDashboardDataAdmin(admin.ModelAdmin):
    list_display = ('user', 'date', 'fsn', 'platform', 'revenue', 'total_spend', 'coupon_error')
    list_filter = ('date', 'user', 'coupon_error')
    search_fields = ('fsn', 'category')
